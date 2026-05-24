package cluster

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const connsPerNode = 64
const poolRequestTimeout = 5 * time.Second
const scannerMaxTokenSize = 64 << 20

type pooledConn struct {
	conn    net.Conn
	scanner *bufio.Scanner
	mu      sync.Mutex
}

type ConnPool struct {
	mu     sync.RWMutex
	pools  map[string]*nodePool // nodeID → connections
	apiKey string
}

type nodePool struct {
	mu    sync.Mutex
	next  atomic.Uint64
	conns []*pooledConn
}

func NewConnPool(apiKey string) *ConnPool {
	return &ConnPool{
		pools:  make(map[string]*nodePool),
		apiKey: apiKey,
	}
}

func (p *ConnPool) get(node Node) (*pooledConn, error) {
	p.mu.RLock()
	np, ok := p.pools[node.ID]
	p.mu.RUnlock()

	if ok {
		return np.get(p, node)
	}

	p.mu.Lock()
	np, ok = p.pools[node.ID]
	if !ok {
		np = &nodePool{}
		p.pools[node.ID] = np
	}
	p.mu.Unlock()

	return np.get(p, node)
}

func (np *nodePool) get(p *ConnPool, node Node) (*pooledConn, error) {
	np.mu.Lock()
	defer np.mu.Unlock()

	if len(np.conns) >= connsPerNode {
		idx := np.next.Add(1) % uint64(len(np.conns))
		return np.conns[idx], nil
	}

	pc, err := p.dial(node)
	if err != nil {
		if len(np.conns) > 0 {
			idx := np.next.Add(1) % uint64(len(np.conns))
			return np.conns[idx], nil
		}
		return nil, err
	}
	np.conns = append(np.conns, pc)
	return pc, nil
}

func (p *ConnPool) dial(node Node) (*pooledConn, error) {
	// neue connection aufbauen
	conn, err := net.DialTimeout("tcp", node.Addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(poolRequestTimeout))

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), scannerMaxTokenSize)

	// auth handshake
	if !scanner.Scan() { // {"auth":"required"}
		conn.Close()
		return nil, fmt.Errorf("auth challenge failed: %v", scanner.Err())
	}
	fmt.Fprintf(conn, "AUTH %s\n", p.apiKey)
	if !scanner.Scan() { // {"auth":"ok"}
		conn.Close()
		return nil, fmt.Errorf("auth response failed: %v", scanner.Err())
	}
	_ = conn.SetDeadline(time.Time{})

	return &pooledConn{conn: conn, scanner: scanner}, nil
}

func (p *ConnPool) Send(node Node, query string) ([]byte, error) {
	pc, err := p.get(node)
	if err != nil {
		return nil, err
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()

	_ = pc.conn.SetDeadline(time.Now().Add(poolRequestTimeout))
	defer pc.conn.SetDeadline(time.Time{})

	_, err = fmt.Fprintf(pc.conn, "%s\n", query)
	if err != nil {
		// connection tot, neu aufbauen
		p.evictConn(node.ID, pc)
		return p.sendFresh(node, query)
	}

	if !pc.scanner.Scan() {
		p.evictConn(node.ID, pc)
		return p.sendFresh(node, query)
	}

	return append([]byte(nil), pc.scanner.Bytes()...), nil
}

func (p *ConnPool) evictConn(nodeID string, dead *pooledConn) {
	p.mu.RLock()
	np, ok := p.pools[nodeID]
	p.mu.RUnlock()
	if !ok {
		return
	}

	np.mu.Lock()
	defer np.mu.Unlock()
	for i, pc := range np.conns {
		if pc == dead {
			_ = pc.conn.Close()
			np.conns = append(np.conns[:i], np.conns[i+1:]...)
			break
		}
	}
}

func (p *ConnPool) sendFresh(node Node, query string) ([]byte, error) {
	pc, err := p.get(node)
	if err != nil {
		return nil, err
	}
	pc.mu.Lock()
	defer pc.mu.Unlock()
	_ = pc.conn.SetDeadline(time.Now().Add(poolRequestTimeout))
	defer pc.conn.SetDeadline(time.Time{})
	fmt.Fprintf(pc.conn, "%s\n", query)
	if !pc.scanner.Scan() {
		return nil, fmt.Errorf("connection failed after retry")
	}
	return append([]byte(nil), pc.scanner.Bytes()...), nil
}

func (p *ConnPool) CloseAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, np := range p.pools {
		for _, pc := range np.conns {
			pc.conn.Close()
		}
	}
	p.pools = make(map[string]*nodePool)
}
