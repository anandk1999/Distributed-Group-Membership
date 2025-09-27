package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"mp2-g02/detectors"
	"mp2-g02/utils"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Controller struct {
	mode utils.DetectionMode
	// gossipManager  *detectors.GossipManager
	pingAckManager *detectors.PingAckManager
	membership     *utils.MembershipList
	network        *utils.NetworkLayer
	suspicionMgr   *utils.SuspicionManager
}

func NewController(config utils.Config) (*Controller, error) {
	network := utils.NewNetworkLayer()
	membership := utils.NewMembershipList(config.NodeID)
	opts := utils.Options{
		SuspicionTimeout:   2 * time.Second,
		CheckInterval:      200 * time.Millisecond,
		RequireReports:     1,
		ConfirmedRetention: 30 * time.Second,
		OnSuspect: func(target utils.NodeID, inc int32, reporters []utils.NodeID) {
			membership.Lock()
			if m, ok := membership.Members[target.String()]; ok {
				m.Status = utils.Suspected
				m.Incarnation = inc
				m.SuspicionStart = time.Now()
			}
			recipients := make([]utils.NodeID, 0, len(membership.Members))
			for _, mm := range membership.Members {
				if mm.ID.String() == membership.LocalNode.String() || mm.ID.String() == target.String() {
					continue
				}
				recipients = append(recipients, mm.ID)
			}
			membership.Unlock()

			msg := utils.Message{
				Type:        utils.Suspect,
				Sender:      membership.LocalNode,
				Target:      target,
				Incarnation: inc,
			}
			for _, r := range recipients {
				network.Send(msg, r.Address())
			}
			// Immediate stdout signal on suspecting node terminal
			fmt.Printf("SUSPECT: target=%s inc=%d reporters=%v by=%s\n", target, inc, reporters, membership.LocalNode)
			log.Printf("[OnSuspect] %s inc=%d reporters=%v", target, inc, reporters)
		},
		OnConfirm: func(target utils.NodeID, inc int32) {
			membership.Lock()
			if m, ok := membership.Members[target.String()]; ok {
				m.Status = utils.Failed
				m.Incarnation = inc
			}
			recipients := make([]utils.NodeID, 0, len(membership.Members))
			for _, mm := range membership.Members {
				if mm.ID.String() == membership.LocalNode.String() || mm.ID.String() == target.String() {
					continue
				}
				recipients = append(recipients, mm.ID)
			}
			membership.Unlock()

			msg := utils.Message{
				Type:        utils.Confirm,
				Sender:      membership.LocalNode,
				Target:      target,
				Incarnation: inc,
			}
			for _, r := range recipients {
				network.Send(msg, r.Address())
			}
			log.Printf("[OnConfirm] %s inc=%d", target, inc)
		},
		OnClear: func(target utils.NodeID, inc int32) {
			membership.Lock()
			if m, ok := membership.Members[target.String()]; ok {
				m.Status = utils.Alive
				if inc >= m.Incarnation {
					m.Incarnation = inc
				}
				m.LastHeartbeat = time.Now()
				m.SuspicionStart = time.Time{}
			}
			recipients := make([]utils.NodeID, 0, len(membership.Members))
			for _, mm := range membership.Members {
				if mm.ID.String() == membership.LocalNode.String() || mm.ID.String() == target.String() {
					continue
				}
				recipients = append(recipients, mm.ID)
			}
			membership.Unlock()

			msg := utils.Message{
				Type:        utils.AliveMsg,
				Sender:      membership.LocalNode,
				Incarnation: inc,
			}
			for _, r := range recipients {
				network.Send(msg, r.Address())
			}
			log.Printf("[OnClear] %s inc=%d", target, inc)
		},
	}

	suspicionMgr := utils.NewSuspicionManager(membership, network, opts)
	// gossipManager := detectors.NewGossipManager(membership, network, suspicionMgr)
	pingAckManager := detectors.NewPingAckManager(membership, network, suspicionMgr)

	controller := &Controller{
		mode: config.Mode,
		// gossipManager: detectors.gossipManager,
		pingAckManager: pingAckManager,
		membership:     membership,
		network:        network,
		suspicionMgr:   suspicionMgr,
	}

	return controller, nil
}

func (c *Controller) Start() error {
	// Start network layer
	if err := c.network.Start(c.membership.LocalNode.Port); err != nil {
		return err
	}

	// Start suspicion manager with a background context
	c.suspicionMgr.Start(context.Background())

	// Start based on mode
	switch c.mode {
	case utils.GossipMode:
		// c.gossipManager.Start()
	case utils.PingAckMode:
		c.pingAckManager.Start()
	}

	log.Printf("Controller started in %v mode", c.mode)
	return nil
}

// JoinGroup joins the distributed group via introducer
func (c *Controller) JoinGroup(introducerAddr string) error {
	switch c.mode {
	case utils.GossipMode:
		// return c.gossipManager.JoinGroup(introducerAddr)
	case utils.PingAckMode:
		return c.pingAckManager.JoinGroup(introducerAddr)
	}
	return nil
}

func (c *Controller) Stop() {
	// Stop managers & network
	switch c.mode {
	case utils.GossipMode:
		// c.gossipManager.Stop()
	case utils.PingAckMode:
		c.pingAckManager.Stop()
	}
	c.suspicionMgr.Stop()
	c.network.Stop()
	log.Println("Controller stopped")
}

func (c *Controller) SetDropRate(rate float32) {
	c.network.SetDropRate(rate)
}

// StartCLI starts a command line interface for the controller
func StartCLI(controller *Controller) {
	// Enhanced CLI to provide basic functionality
	log.Println("CLI started - Controller is running")
	log.Printf("Node ID: %s", controller.membership.LocalNode)
	log.Printf("Listening on port: %d", controller.membership.LocalNode.Port)

	// Periodically log membership status
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		members := controller.membership.GetAllMembers()
		aliveCount := 0
		suspectedCount := 0
		failedCount := 0

		log.Printf("=== MEMBERSHIP STATUS ===")
		log.Printf("Total members: %d", len(members))

		for _, member := range members {
			timeSinceHeartbeat := time.Since(member.LastHeartbeat)
			statusInfo := ""

			switch member.Status {
			case utils.Alive:
				aliveCount++
				if timeSinceHeartbeat > 5*time.Second {
					statusInfo = fmt.Sprintf(" (stale: %v)", timeSinceHeartbeat)
				}
			case utils.Suspected:
				suspectedCount++
				timeSinceSuspicion := time.Since(member.SuspicionStart)
				statusInfo = fmt.Sprintf(" (suspected for: %v)", timeSinceSuspicion)
			case utils.Failed:
				failedCount++
				statusInfo = " (failed)"
			}

			log.Printf("  %s | %s | Inc:%d | LastHB:%v ago%s",
				member.ID, member.Status, member.Incarnation,
				timeSinceHeartbeat.Truncate(time.Millisecond), statusInfo)
		}

		log.Printf("Summary: %d alive, %d suspected, %d failed", aliveCount, suspectedCount, failedCount)

		// Log recent updates being propagated
		recentUpdates := controller.membership.GetRecentUpdates(10)
		if len(recentUpdates) > 0 {
			log.Printf("Recent updates (piggybacking): %d", len(recentUpdates))
			for _, update := range recentUpdates {
				log.Printf("%s -> %s (Inc:%d)", update.NodeID, update.Status, update.Incarnation)
			}
		}
		log.Printf("========================")
	}
}

func (c *Controller) SwitchMode(mode utils.DetectionMode) {
	c.mode = mode

	switch mode {
	case utils.GossipMode:
		c.pingAckManager.Stop()
		// c.gossipManager.Start()
	case utils.PingAckMode:
		// c.gossipManager.Stop()
		c.pingAckManager.Start()
	}
}

// Enable or disable suspicion in the current protocol manager
func (c *Controller) SetSuspicion(enable bool) {
	switch c.mode {
	case utils.PingAckMode:
		c.pingAckManager.SetSuspicion(enable)
	case utils.GossipMode:
		// c.gossipManager.SetSuspicion(enable)
	}
}

func (c *Controller) GetProtocol() (string, string) {
	mech := "gossip"
	suspect := "nosuspect"
	if c.mode == utils.PingAckMode {
		mech = "ping"
		if c.pingAckManager != nil {
			if c.pingAckManager.SuspicionEnabled() {
				suspect = "suspect"
			}
		}
	}
	if c.mode == utils.GossipMode {
		mech = "gossip"
		// if c.gossipManager != nil {
		// 	if c.gossipManager.SuspicionEnabled() {
		// 		suspect = "suspect"
		// 	}
		// }
	}
	return mech, suspect
}

func (c *Controller) LeaveGroup() {
	switch c.mode {
	case utils.PingAckMode:
		c.pingAckManager.LeaveGroup()
	case utils.GossipMode:
		// c.gossipManager.LeaveGroup()
	}
}

func main() {
	var (
		port          = flag.Int("port", 8080, "UDP port to listen on")
		introducerIP  = flag.String("introducer", "", "Introducer IP:Port")
		isIntroducer  = flag.Bool("is-introducer", false, "Act as introducer")
		mode          = flag.String("mode", "gossip", "Detection mode: gossip or pingack")
		cmd           = flag.String("cmd", "", "Client command: list_mem, list_self, join, leave, display_suspects, switch, display_protocol")
		controlPortIn = flag.Int("control-port", 0, "Control server port on localhost (default: port+10000)")
		arg1          = flag.String("arg1", "", "Optional argument 1 for cmd")
		arg2          = flag.String("arg2", "", "Optional argument 2 for cmd")
		foreground    = flag.Bool("foreground", false, "Run in foreground (do not daemonize)")
	)
	flag.Parse()

	controlPort := *controlPortIn
	if controlPort == 0 {
		controlPort = *port + 10000
	}

	// If -cmd is provided, act as a client and exit
	if *cmd != "" {
		runClient(*cmd, controlPort, *arg1, *arg2)
		return
	}

	// Daemonize (background) unless foreground requested or already daemonized
	if !*foreground && os.Getenv("MP2_DAEMONIZED") != "1" {
		exe, err := os.Executable()
		if err != nil {
			log.Fatalf("cannot get executable: %v", err)
		}
		args := os.Args[1:]
		child := exec.Command(exe, args...)
		child.Env = append(os.Environ(), "MP2_DAEMONIZED=1")
		child.Stdin = nil
		if err := child.Start(); err != nil {
			log.Fatalf("failed to start daemon: %v", err)
		}

		fmt.Printf("Started mp2-node daemon pid=%d port=%d (control-port=%d)\n", child.Process.Pid, *port, controlPort)
		return
	}

	// Get local IP
	localIP := utils.GetLocalIP()
	nodeID := utils.NodeID{
		IP:        localIP,
		Port:      *port,
		Timestamp: time.Now().Unix(),
	}

	// Create controller
	config := utils.Config{
		NodeID:         nodeID,
		IntroducerAddr: *introducerIP,
		IsIntroducer:   *isIntroducer,
		Mode:           utils.ParseMode(*mode),
	}

	controller, err := NewController(config)
	if err != nil {
		log.Fatalf("Failed to create controller: %v", err)
	}

	// Start the system
	if err := controller.Start(); err != nil {
		log.Fatalf("Failed to start controller: %v", err)
	}

	// Start control server (daemon API)
	ctl := NewControlServer(controller, controlPort)
	ctl.Start()

	// Join the group if not introducer
	if !*isIntroducer && *introducerIP != "" {
		if err := controller.JoinGroup(*introducerIP); err != nil {
			log.Printf("Failed to join group: %v", err)
		} else {
			log.Printf("Joined the group")
		}
	}

	go StartCLI(controller)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")
	// stop control server first
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctl.Stop(shutdownCtx)
	controller.Stop()
}

type ControlServer struct {
	controller *Controller
	srv        *http.Server
}

func NewControlServer(c *Controller, controlPort int) *ControlServer {
	mux := http.NewServeMux()
	cs := &ControlServer{controller: c}

	mux.HandleFunc("/list_mem", cs.handleListMem)
	mux.HandleFunc("/list_self", cs.handleListSelf)
	mux.HandleFunc("/display_suspects", cs.handleDisplaySuspects)
	mux.HandleFunc("/join", cs.handleJoin)
	mux.HandleFunc("/leave", cs.handleLeave)
	mux.HandleFunc("/switch", cs.handleSwitch)
	mux.HandleFunc("/display_protocol", cs.handleDisplayProtocol)

	cs.srv = &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%d", controlPort),
		Handler: mux,
	}
	return cs
}

func (cs *ControlServer) Start() {
	go func() {
		log.Printf("Control server listening on http://%s", cs.srv.Addr)
		if err := cs.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("control server error: %v", err)
		}
	}()
}

func (cs *ControlServer) Stop(ctx context.Context) {
	_ = cs.srv.Shutdown(ctx)
}

func (cs *ControlServer) handleListMem(w http.ResponseWriter, r *http.Request) {
	members := cs.controller.membership.GetAllMembers()
	for _, member := range members {
		if member.Status == utils.Alive {
			fmt.Fprintf(w, "  %s | %s | Inc:%d\n", member.ID, member.Status, member.Incarnation)
		}
	}
}

func (cs *ControlServer) handleListSelf(w http.ResponseWriter, r *http.Request) {
	member := cs.controller.membership.Members[cs.controller.membership.LocalNode.String()]
	fmt.Fprintf(w, "  %s | %s | Inc:%d\n", member.ID, member.Status, member.Incarnation)
}

func (cs *ControlServer) handleDisplaySuspects(w http.ResponseWriter, r *http.Request) {
	suspects := cs.controller.membership.GetSuspectedMembers()
	for _, member := range suspects {
		fmt.Fprintf(w, "  %s | %s | Inc:%d\n", member.ID, member.Status, member.Incarnation)
	}
}

func (cs *ControlServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	intro := q.Get("introducer")
	if intro == "" {
		http.Error(w, "introducer is required", http.StatusBadRequest)
		return
	}
	if err := cs.controller.JoinGroup(intro); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "ok\n")
}

func (cs *ControlServer) handleLeave(w http.ResponseWriter, r *http.Request) {
	cs.controller.LeaveGroup()
	io.WriteString(w, "ok\n")
}

func (cs *ControlServer) handleSwitch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	mech := strings.ToLower(q.Get("mechanism"))
	susp := strings.ToLower(q.Get("suspicion"))

	if mech != "gossip" && mech != "ping" {
		http.Error(w, "mechanism must be 'gossip' or 'ping'", http.StatusBadRequest)
		return
	}
	if susp != "suspect" && susp != "nosuspect" {
		http.Error(w, "suspicion must be 'suspect' or 'nosuspect'", http.StatusBadRequest)
		return
	}

	if mech == "ping" {
		cs.controller.SwitchMode(utils.PingAckMode)
	} else {
		cs.controller.SwitchMode(utils.GossipMode)
	}
	cs.controller.SetSuspicion(susp == "suspect")
	io.WriteString(w, "ok\n")
}

func (cs *ControlServer) handleDisplayProtocol(w http.ResponseWriter, r *http.Request) {
	mech, susp := cs.controller.GetProtocol()
	fmt.Fprintf(w, "(%s, %s)", mech, susp)
}

func runClient(cmd string, controlPort int, arg1, arg2 string) {
	base := fmt.Sprintf("http://127.0.0.1:%d", controlPort)
	var endpoint string
	switch strings.ToLower(cmd) {
	case "list_mem":
		endpoint = "/list_mem"
	case "list_self":
		endpoint = "/list_self"
	case "display_suspects":
		endpoint = "/display_suspects"
	case "join":
		if arg1 == "" {
			log.Fatal("join requires arg1=introducer ip:port")
		}
		endpoint = "/join?" + url.Values{"introducer": {arg1}}.Encode()
	case "leave":
		endpoint = "/leave"
	case "switch":
		// arg1=mechanism (gossip|ping), arg2=suspect|nosuspect
		if arg1 == "" || arg2 == "" {
			log.Fatal("switch requires arg1={gossip|ping} arg2={suspect|nosuspect}")
		}
		endpoint = "/switch?" + url.Values{"mechanism": {arg1}, "suspicion": {arg2}}.Encode()
	case "display_protocol":
		endpoint = "/display_protocol"
	default:
		log.Fatalf("unknown cmd: %s", cmd)
	}
	resp, err := http.Get(base + endpoint)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}
