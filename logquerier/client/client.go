package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mp2-g02/logquerier/common"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Options struct {
	Hosts     []string
	HostsFile string
	Port      int
	Request   common.ServerRequest
	Output    io.Writer
	ErrOutput io.Writer
	Timeout   time.Duration
}

type Result struct {
	Addr    string
	LogFile string
	Line    string
	Err     error
}

func Run(ctx context.Context, opts Options) error {
	if opts.Output == nil {
		opts.Output = os.Stdout
	}
	if opts.ErrOutput == nil {
		opts.ErrOutput = os.Stderr
	}
	if opts.Port == 0 {
		opts.Port = common.DefaultPort
	}
	if opts.Timeout == 0 {
		opts.Timeout = 5 * time.Second
	}

	addresses, err := resolveHosts(opts.Hosts, opts.HostsFile)
	if err != nil {
		return err
	}
	if len(addresses) == 0 {
		return fmt.Errorf("log querier: no hosts available")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	combined := fanInConnections(ctx, addresses, opts.Port, opts.Request, opts.Timeout)

	shouldCount := !strings.Contains(opts.Request.Input, "-c") && !strings.Contains(opts.Request.Input, "--count")
	total := 0

	for res := range combined {
		if res.Err != nil {
			fmt.Fprintf(opts.ErrOutput, "log querier: %s error: %v\n", res.Addr, res.Err)
			continue
		}
		fmt.Fprintf(opts.Output, "[%s %s] %s\n", res.Addr, res.LogFile, res.Line)
		if shouldCount {
			total++
		}
	}

	if shouldCount {
		fmt.Fprintf(opts.Output, "Total matches: %d\n", total)
	}

	return nil
}

func resolveHosts(explicit []string, hostsFile string) ([]string, error) {
	hosts := make([]string, 0)
	hosts = append(hosts, explicit...)

	if hostsFile != "" {
		file, err := os.Open(hostsFile)
		if err != nil {
			return nil, fmt.Errorf("log querier: open hosts file: %w", err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			raw := strings.TrimSpace(scanner.Text())
			if raw == "" || strings.HasPrefix(raw, "#") {
				continue
			}
			hosts = append(hosts, raw)
		}
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("log querier: read hosts file: %w", err)
		}
	}

	unique := make(map[string]struct{})
	for _, h := range hosts {
		unique[h] = struct{}{}
	}

	dedup := make([]string, 0, len(unique))
	for h := range unique {
		dedup = append(dedup, h)
	}
	sort.Strings(dedup)
	return dedup, nil
}

func fanInConnections(ctx context.Context, hosts []string, port int, req common.ServerRequest, timeout time.Duration) <-chan Result {
	channels := make([]<-chan Result, 0, len(hosts))
	for _, host := range hosts {
		address := host
		if !strings.Contains(host, ":") {
			address = net.JoinHostPort(host, strconv.Itoa(port))
		}
		channels = append(channels, connection(ctx, address, req, timeout))
	}
	return fanIn(channels...)
}

func connection(ctx context.Context, address string, req common.ServerRequest, timeout time.Duration) <-chan Result {
	ch := make(chan Result)

	go func() {
		defer close(ch)

		d := &net.Dialer{Timeout: timeout}
		conn, err := d.DialContext(ctx, "tcp", address)
		if err != nil {
			ch <- Result{Addr: address, Err: err}
			return
		}
		defer conn.Close()

		_ = conn.SetDeadline(time.Now().Add(timeout))
		encoder := json.NewEncoder(conn)
		if err := encoder.Encode(req); err != nil {
			ch <- Result{Addr: address, Err: err}
			return
		}
		_ = conn.SetDeadline(time.Time{})

		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			line := scanner.Text()
			var resp common.ServerResponse
			if err := json.Unmarshal([]byte(line), &resp); err != nil {
				ch <- Result{Addr: address, Err: err}
				continue
			}

			select {
			case <-ctx.Done():
				return
			case ch <- Result{Addr: address, LogFile: resp.LogFile, Line: resp.Output}:
			}
		}
		if err := scanner.Err(); err != nil {
			ch <- Result{Addr: address, Err: err}
		}
	}()

	return ch
}

func fanIn(channels ...<-chan Result) <-chan Result {
	out := make(chan Result)
	var wg sync.WaitGroup
	wg.Add(len(channels))

	for _, ch := range channels {
		go func(c <-chan Result) {
			defer wg.Done()
			for res := range c {
				out <- res
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func DefaultHostsFile() string {
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		candidate := filepath.Join(dir, "hosts.txt")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	if wd, err := os.Getwd(); err == nil {
		candidate := filepath.Join(wd, "hosts.txt")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return "hosts.txt"
}
