package main

import (
	"bufio"
	"context"
	"log"
	"net"
	"sync"
	"time"
)

type RateLimiter interface {
	Allow(ip string) bool
}

type ProxyConfig struct {
	ListenPort        string
	TargetHost        string
	TargetPort        string
	MaxConnPerIP      int
	RateLimit         int
	RateLimitWindow   time.Duration
	ConnectionTimeout time.Duration
}

type DDoSProxy struct {
	config      ProxyConfig
	rateLimiter RateLimiter
	listener    net.Listener
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
}

type IPRateLimiter struct {
	ips          map[string]int
	mu           sync.Mutex
	rateLimit    int
	window       time.Duration
	lastCleanup  time.Time
	cleanupEvery time.Duration
}

func NewIPRateLimiter(rateLimit int, window time.Duration) *IPRateLimiter {
	return &IPRateLimiter{
		ips:          make(map[string]int),
		rateLimit:    rateLimit,
		window:       window,
		lastCleanup:  time.Now(),
		cleanupEvery: window * 2,
	}
}

func (l *IPRateLimiter) Allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if time.Since(l.lastCleanup) > l.cleanupEvery {
		for ip := range l.ips {
			delete(l.ips, ip)
		}
		l.lastCleanup = time.Now()
	}

	count := l.ips[ip] + 1
	if count > l.rateLimit {
		return false
	}

	l.ips[ip] = count
	time.AfterFunc(l.window, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.ips[ip] > 0 {
			l.ips[ip]--
		}
	})

	return true
}

func NewDDoSProxy(config ProxyConfig) *DDoSProxy {
	ctx, cancel := context.WithCancel(context.Background())
	return &DDoSProxy{
		config:      config,
		rateLimiter: NewIPRateLimiter(config.RateLimit, config.RateLimitWindow),
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (p *DDoSProxy) Start() error {
	var err error
	p.listener, err = net.Listen("tcp", ":"+p.config.ListenPort)
	if err != nil {
		return err
	}

	log.Printf("Proxy started on port %s, forwarding to %s:%s\n",
		p.config.ListenPort, p.config.TargetHost, p.config.TargetPort)

	p.wg.Add(1)
	go p.acceptConnections()

	return nil
}

func (p *DDoSProxy) Stop() {
	p.cancel()
	p.listener.Close()
	p.wg.Wait()
	log.Println("Proxy stopped")
}

func (p *DDoSProxy) acceptConnections() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
			conn, err := p.listener.Accept()
			if err != nil {
				log.Println("Accept error:", err)
				continue
			}

			p.wg.Add(1)
			go p.handleConnection(conn)
		}
	}
}

func (p *DDoSProxy) handleConnection(clientConn net.Conn) {
	defer p.wg.Done()
	defer clientConn.Close()

	clientIP, _, err := net.SplitHostPort(clientConn.RemoteAddr().String())
	if err != nil {
		log.Println("Error parsing client address:", err)
		return
	}

	if !p.rateLimiter.Allow(clientIP) {
		log.Printf("Rate limit exceeded for IP: %s\n", clientIP)
		clientConn.Write([]byte("HTTP/1.1 429 Too Many Requests\r\n\r\n"))
		return
	}

	ctx, cancel := context.WithTimeout(p.ctx, p.config.ConnectionTimeout)
	defer cancel()

	targetAddr := net.JoinHostPort(p.config.TargetHost, p.config.TargetPort)
	targetConn, err := net.DialTimeout("tcp", targetAddr, p.config.ConnectionTimeout)
	if err != nil {
		log.Println("Target connection error:", err)
		return
	}
	defer targetConn.Close()

	done := make(chan struct{}, 2)

	go func() {
		p.copyData(ctx, clientConn, targetConn)
		done <- struct{}{}
	}()

	go func() {
		p.copyData(ctx, targetConn, clientConn)
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (p *DDoSProxy) copyData(ctx context.Context, src, dst net.Conn) {
	buf := make([]byte, 32*1024)
	reader := bufio.NewReader(src)
	writer := bufio.NewWriter(dst)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			src.SetReadDeadline(time.Now().Add(p.config.ConnectionTimeout))
			n, err := reader.Read(buf)
			if err != nil {
				return
			}

			dst.SetWriteDeadline(time.Now().Add(p.config.ConnectionTimeout))
			_, err = writer.Write(buf[:n])
			if err != nil {
				return
			}
			writer.Flush()
		}
	}
}

func main() {
	config := ProxyConfig{
		ListenPort:        "8080",
		TargetHost:        "example.com",
		TargetPort:        "80",
		MaxConnPerIP:      100,
		RateLimit:         1000,
		RateLimitWindow:   time.Minute,
		ConnectionTimeout: 10 * time.Second,
	}

	proxy := NewDDoSProxy(config)
	if err := proxy.Start(); err != nil {
		log.Fatal("Failed to start proxy:", err)
	}

	stop := make(chan struct{})
	<-stop

	proxy.Stop()
}
