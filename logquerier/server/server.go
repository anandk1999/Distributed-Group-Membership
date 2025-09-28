package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"mp2-g02/logquerier/common"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

type Config struct {
	Addr             string
	LogPaths         map[string]string
	DefaultFileType  string
	WorkingDirectory string
}

type Server struct {
	cfg      Config
	mu       sync.Mutex
	listener net.Listener
}

func New(cfg Config) (*Server, error) {
	if cfg.Addr == "" {
		cfg.Addr = fmt.Sprintf(":%d", common.DefaultPort)
	}
	if cfg.DefaultFileType == "" {
		cfg.DefaultFileType = common.DefaultFileType
	}
	if cfg.LogPaths == nil {
		cfg.LogPaths = map[string]string{}
	}
	if _, ok := cfg.LogPaths[cfg.DefaultFileType]; !ok {
		return nil, fmt.Errorf("log path for default file type %q not configured", cfg.DefaultFileType)
	}
	return &Server{cfg: cfg}, nil
}

func (s *Server) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	log.Printf("log querier server listening on %s", ln.Addr().String())

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Printf("temporary accept error: %v", err)
				continue
			}
			return err
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	var req common.ServerRequest
	if err := decoder.Decode(&req); err != nil {
		log.Printf("log server: decode error: %v", err)
		return
	}

	logPath := s.resolveLogPath(req.FileType)
	if logPath == "" {
		log.Printf("log server: unknown file type %q", req.FileType)
		return
	}

	if _, err := os.Stat(logPath); err != nil {
		log.Printf("log server: log file %s unavailable: %v", logPath, err)
		return
	}

	s.streamGrep(conn, strings.TrimSpace(req.Input), logPath)
}

func (s *Server) resolveLogPath(fileType string) string {
	if fileType != "" {
		if p, ok := s.cfg.LogPaths[fileType]; ok {
			if !filepath.IsAbs(p) {
				if s.cfg.WorkingDirectory != "" {
					return filepath.Join(s.cfg.WorkingDirectory, p)
				}
				abs, err := filepath.Abs(p)
				if err == nil {
					return abs
				}
			}
			return p
		}
	}
	if p, ok := s.cfg.LogPaths[s.cfg.DefaultFileType]; ok {
		if !filepath.IsAbs(p) {
			if s.cfg.WorkingDirectory != "" {
				return filepath.Join(s.cfg.WorkingDirectory, p)
			}
			abs, err := filepath.Abs(p)
			if err == nil {
				return abs
			}
		}
		return p
	}
	return ""
}

func (s *Server) streamGrep(conn net.Conn, query, logPath string) {
	name, args := parseCommand(query)
	if name == "" {
		log.Printf("log server: empty command")
		return
	}
	if name != "grep" {
		log.Printf("log server: ignoring non-grep command %q", query)
		return
	}

	args = append(args, logPath)
	cmd := exec.Command(name, args...)
	if s.cfg.WorkingDirectory != "" {
		cmd.Dir = s.cfg.WorkingDirectory
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Printf("log server: stdout pipe error: %v", err)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Printf("log server: stderr pipe error: %v", err)
		return
	}

	if err := cmd.Start(); err != nil {
		log.Printf("log server: failed to start grep: %v", err)
		return
	}

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Printf("log server (grep stderr): %s", scanner.Text())
		}
	}()

	outScanner := bufio.NewScanner(stdout)
	encoder := json.NewEncoder(conn)
	for outScanner.Scan() {
		resp := common.ServerResponse{Output: outScanner.Text(), LogFile: logPath}
		if err := encoder.Encode(&resp); err != nil {
			log.Printf("log server: encode response error: %v", err)
			break
		}
	}
	if err := outScanner.Err(); err != nil {
		log.Printf("log server: read stdout error: %v", err)
	}

	if err := cmd.Wait(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() != 1 { // grep returns 1 when no matches
				log.Printf("log server: grep exited with code %d", exitErr.ExitCode())
			}
		} else {
			log.Printf("log server: wait error: %v", err)
		}
	}
}

func parseCommand(input string) (string, []string) {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return "", nil
	}
	return fields[0], fields[1:]
}
