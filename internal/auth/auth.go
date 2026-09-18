package auth

import "github.com/byPixelTV/flamedb/internal/config"

type Permission string

const (
	PermInternal Permission = "internal"
	PermRead     Permission = "read"
	PermWrite    Permission = "write"
)

type Session struct {
	Name        string
	Permissions map[Permission]bool
}

type Auth struct {
	keys map[string]*Session
}

func New(cfg config.AuthConfig) *Auth {
	a := &Auth{keys: make(map[string]*Session)}
	for _, k := range cfg.Keys {
		perms := make(map[Permission]bool)
		for _, p := range k.Permissions {
			perms[Permission(p)] = true
		}
		a.keys[k.Key] = &Session{
			Name:        k.Name,
			Permissions: perms,
		}
	}
	if cfg.EffectiveInternalKey() != "" {
		a.keys[cfg.EffectiveInternalKey()] = &Session{Name: "cluster", Permissions: map[Permission]bool{PermRead: true, PermWrite: true, PermInternal: true}}
	}
	return a
}

func (a *Auth) Validate(key string) (*Session, bool) {
	session, ok := a.keys[key]
	return session, ok
}

func (s *Session) Can(p Permission) bool {
	return s.Permissions[p]
}
