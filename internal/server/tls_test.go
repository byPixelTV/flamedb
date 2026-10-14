package server

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/byPixelTV/flamedb/internal/auth"
	"github.com/byPixelTV/flamedb/internal/config"
)

func TestSharedTLSWithUnchangedAuthAndCommands(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath := filepath.Join(dir, "server.pem")
	keyPath := filepath.Join(dir, "server-key.pem")
	if err = os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: private}), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := probe.Addr().String()
	probe.Close()
	cfg := &config.Config{Server: config.ServerConfig{TLSCert: certPath, TLSKey: keyPath}, Auth: config.AuthConfig{Keys: []config.APIKey{{Name: "legacy", Key: "legacy", Permissions: []string{"read", "write"}}}}}
	e := &blockingEngine{entered: make(chan struct{}, 1), release: make(chan struct{})}
	close(e.release)
	srv := NewShared(e, auth.New(cfg.Auth), cfg)
	defer srv.Close()
	serve := make(chan error, 1)
	go func() { serve <- srv.Listen(addr) }()
	var client *tls.Conn
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); {
		client, err = tls.DialWithDialer(&net.Dialer{Timeout: 100 * time.Millisecond}, "tcp", addr, &tls.Config{RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS12})
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	client.SetDeadline(time.Now().Add(5 * time.Second))
	scanner := bufio.NewScanner(client)
	if !scanner.Scan() || scanner.Text() != `{"auth":"required"}` {
		t.Fatal("TLS greeting", scanner.Text(), scanner.Err())
	}
	if result := frame(t, client, scanner, "AUTH legacy"); result["auth"] != "ok" {
		t.Fatal(result)
	}
	if result := frame(t, client, scanner, "WRITE app:visits 1"); result["error"] != nil {
		t.Fatal(result)
	}
	if state := client.ConnectionState(); !state.HandshakeComplete || len(state.VerifiedChains) == 0 {
		t.Fatal("TLS not authenticated")
	}
}
