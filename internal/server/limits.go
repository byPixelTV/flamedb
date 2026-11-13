package server

func (s *Server) connectionLimit() int {
	if s.cfg != nil && s.cfg.Server.MaxConnections > 0 {
		return s.cfg.Server.MaxConnections
	}
	return 1024
}
func (s *Server) requestLimit() int {
	if s.cfg != nil && s.cfg.Server.MaxRequestBytes > 0 {
		return s.cfg.Server.MaxRequestBytes
	}
	return 8 << 20
}
func (s *Server) batchLimit() int {
	if s.cfg != nil && s.cfg.Server.MaxBatchEntries > 0 {
		return s.cfg.Server.MaxBatchEntries
	}
	return 10000
}
