package serviceD

import (
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type raftNodeInfo struct {
	raft           *raft.Raft
	fsm            *FSM
	leaderNotifyCh chan bool
}

func newRaftNode(s *serviceD) (*raftNodeInfo, error) {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(s.opts.raftTCPAddress)
	raftConfig.Logger = log.New(os.Stderr, "[ serviceD ]:", log.LstdFlags)
	raftConfig.SnapshotInterval = 30 * time.Second
	//raftConfig.SnapshotThreshold = 2
	leaderNotifyCh := make(chan bool, 1)
	raftConfig.NotifyCh = leaderNotifyCh

	addr, err := net.ResolveTCPAddr("tcp", s.opts.raftTCPAddress)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(s.opts.raftTCPAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(s.opts.dataDir, 0700); err != nil {
		return nil, err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(s.opts.dataDir, 1, os.Stderr)
	if err != nil {
		return nil, err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(s.opts.dataDir, "raft-log.bolt"))
	if err != nil {
		return nil, err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(s.opts.dataDir, "raft-stable.bolt"))
	if err != nil {
		return nil, err
	}

	raftNode, err := raft.NewRaft(raftConfig, (*FSM)(s), logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, err
	}

	if s.opts.bootstrap && s.opts.slaveOf == "" {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		raftNode.BootstrapCluster(configuration)
	}

	return &raftNodeInfo{raft: raftNode, fsm: (*FSM)(s), leaderNotifyCh: leaderNotifyCh}, nil
}

// joinRaftCluster joins a node to raftNode cluster
func joinRaftCluster(opts *options) error {
	url := fmt.Sprintf("http://%s/join?peerAddress=%s", opts.slaveOf, opts.raftTCPAddress)

	resp, err := http.Get(url)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if string(body) != "ok" {
		return errors.New(fmt.Sprintf("Error joining cluster: %s", body))
	}

	return nil
}