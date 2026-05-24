package cluster

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

type pooledConn struct {
	conn    net.Conn
	scanner *bufio.Scanner
	mu      sync.Mutex
}

type ConnPool struct {
	mu     sync.RWMutex
	pools  map[string]*pooledConn // nodeID → conn
	apiKey string
}

func NewConnPool(apiKey string) *ConnPool {
	return &ConnPool{
		pools:  make(map[string]*pooledConn),
		apiKey: apiKey,
	}
}

func (p *ConnPool) get(node Node) (*pooledConn, error) {
	p.mu.RLock()
	pc, ok := p.pools[node.ID]
	p.mu.RUnlock()

	if ok {
		return pc, nil
	}

	// neue connection aufbauen
	conn, err := net.DialTimeout("tcp", node.Addr, 3*time.Second)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(conn)

	// auth handshake
	scanner.Scan() // {"auth":"required"}
	fmt.Fprintf(conn, "AUTH %s\n", p.apiKey)
	scanner.Scan() // {"auth":"ok"}

	pc = &pooledConn{conn: conn, scanner: scanner}

	p.mu.Lock()
	p.pools[node.ID] = pc
	p.mu.Unlock()

	return pc, nil
}

func (p *ConnPool) Send(node Node, query string) ([]byte, error) {
	pc, err := p.get(node)
	if err != nil {
		return nil, err
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	_, err = fmt.Fprintf(pc.conn, "%s\n", query)
	if err != nil {
		// connection tot, neu aufbauen
		p.evict(node.ID)
		return p.sendFresh(node, query)
	}

	if !pc.scanner.Scan() {
		p.evict(node.ID)
		return p.sendFresh(node, query)
	}

	return pc.scanner.Bytes(), nil
}

func (p *ConnPool) evict(nodeID string) {
	p.mu.Lock()
	if pc, ok := p.pools[nodeID]; ok {
		pc.conn.Close()
		delete(p.pools, nodeID)
	}
	p.mu.Unlock()
}

func (p *ConnPool) sendFresh(node Node, query string) ([]byte, error) {
	pc, err := p.get(node)
	if err != nil {
		return nil, err
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	fmt.Fprintf(pc.conn, "%s\n", query)
	if !pc.scanner.Scan() {
		return nil, fmt.Errorf("connection failed after retry")
	}
	return pc.scanner.Bytes(), nil
}

func (p *ConnPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pc := range p.pools {
		pc.conn.Close()
	}
	p.pools = make(map[string]*pooledConn)
}
