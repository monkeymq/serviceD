package serviceD

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

type options struct {
	dataDir        string
	httpAddress    string
	raftTCPAddress string
	slaveOf        string
	bootstrap      bool
	ttl            int64
}

type payload struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type serviceD struct {
	mu     sync.Mutex
	opts   *options
	logger *log.Logger
	m      sync.Map // The key-value store for the system.

	raftNode *raftNodeInfo // The consensus mechanism
	hs       *httpServer
}

func New(opts *options) *serviceD {
	return &serviceD{
		opts:   opts,
		logger: log.New(os.Stderr, "[ serviceD ]: ", log.LstdFlags),
	}
}
func (s *serviceD) Open() error {
	var l net.Listener
	var err error
	l, err = net.Listen("tcp", s.opts.httpAddress)
	if err != nil {
		s.logger.Fatal(fmt.Sprintf("Listen %s failed: %s \n", s.opts.httpAddress, err))
	}
	s.logger.Printf("HTTP server listen to: %s", l.Addr())

	logger := log.New(os.Stderr, "[HTTP server]: ", log.LstdFlags)
	httpServer := NewHttpServer(s, logger)
	s.hs = httpServer
	go func() {
		http.Serve(l, httpServer.mux)
	}()

	raft, err := newRaftNode(s)
	if err != nil {
		s.logger.Fatal(fmt.Sprintf("New raftNode failed:%v", err))
	}
	s.raftNode = raft

	if s.opts.slaveOf != "" {
		go func() {
			for {
				s.logger.Print("------------\n")
				err = joinRaftCluster(s.opts)
				if err != nil {
					s.logger.Print(fmt.Sprintf("Join raftNode cluster failed: %v", err))
					time.Sleep(3 * time.Second)
				} else {
					break
				}
			}
		}()
	}

	// monitor leadership
	for {
		select {
		case leader := <-s.raftNode.leaderNotifyCh:
			if leader {
				s.logger.Println("========== Become leader ==========")
				s.hs.setWriteable(true)
			} else {
				s.logger.Println("========== Become follower ==========")
				s.hs.setWriteable(false)
			}
		}
	}

	return err
}

func NewOptions() *options {
	var httpPort = flag.String("http", "16000", "Http port is used to provide storage service")
	var tcpPort = flag.String("tcp", "17000", "Tcp port is used for communication between nodes")
	var node = flag.String("name", "raft-node", "Node name")
	var bootstrap = flag.Bool("bootstrap", false, "Start as cluster leader if true")
	var slaveOf = flag.String("slaveof", "", "Leader's http address")
	var ttl = flag.Int64("ttl", 30, "Key expiration time in second")
	var dir = flag.String("dir", ".", "A directory to save data and logs")
	var version = flag.Bool("v", false, "Show version")

	flag.Parse()

	if *version {
		fmt.Fprintf(os.Stdout, "1.0.0\n")
		os.Exit(0)
	}

	opts := &options{}
	localIP := getLocalIP() + ":"

	opts.dataDir = *dir + "/" + *node
	//os.RemoveAll(opts.dataDir)
	opts.httpAddress = localIP + *httpPort
	opts.bootstrap = *bootstrap
	opts.raftTCPAddress = localIP + *tcpPort
	opts.slaveOf = *slaveOf
	opts.ttl = *ttl
	return opts
}

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

func getNowInSencond() int64 {
	return time.Now().Unix()
}
