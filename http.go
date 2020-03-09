package serviceD

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

const (
	WRITEABLE         = int32(1)
	READONLY          = int32(0)
	WRITEABLE_ERR_MSG = "Forbidden\n"
)

type httpServer struct {
	s           *serviceD
	log         *log.Logger
	mux         *http.ServeMux
	enableWrite int32
}

func NewHttpServer(s *serviceD, log *log.Logger) *httpServer {
	mux := http.NewServeMux()
	hs := &httpServer{
		s:           s,
		log:         log,
		mux:         mux,
		enableWrite: READONLY,
	}

	mux.HandleFunc("/set", hs.doSet)
	mux.HandleFunc("/get", hs.doGet)
	mux.HandleFunc("/join", hs.doJoin)
	mux.HandleFunc("/delete", hs.doDel)
	mux.HandleFunc("/leader", hs.doLeader)
	mux.HandleFunc("/list", hs.doList)
	return hs
}

func (h *httpServer) checkWritePermission() bool {
	return atomic.LoadInt32(&h.enableWrite) == WRITEABLE
}

func (h *httpServer) setWriteable(flag bool) {
	if flag {
		atomic.StoreInt32(&h.enableWrite, WRITEABLE)
	} else {
		atomic.StoreInt32(&h.enableWrite, READONLY)
	}
}

func (h *httpServer) doGet(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()

	key := vars.Get("key")
	if key == "" {
		h.log.Println("doGet() error, key is null")
		fmt.Fprint(w, "")
		return
	}

	var ret = "[]"
	hostTime, _ := h.s.m.Load(key)
	availableService := make([]string, 0, 16)
	expiredService := make([]string, 0, 16)
	if hostTime != nil {
		sec := getNowInSencond()
		for host, time := range hostTime.(hostMapTime) {
			if (sec - time) < h.s.opts.ttl {
				availableService = append(availableService, host)
			} else {
				expiredService = append(expiredService, host)
			}
		}
	}
	if len(expiredService) > 0 {
		go func(expiredService []string, hostTime hostMapTime) {
			for _, e := range expiredService {
				delete(hostTime, e)
			}
		}(expiredService, hostTime.(hostMapTime))
	}
	data, err := json.Marshal(availableService)
	if err != nil {
		h.log.Fatalf("json Marshal is failed. availiableService is : %v\n", availableService)
	}
	ret = string(data)
	fmt.Fprintf(w, "%s\n", ret)
}

// doSet saves data to cache, only raftNode master node provides this api
func (h *httpServer) doSet(w http.ResponseWriter, r *http.Request) {
	if !h.checkWritePermission() {
		fmt.Fprint(w, WRITEABLE_ERR_MSG)
		return
	}
	vars := r.URL.Query()

	fmt.Printf("vars is ========= %v \n\n", vars)

	key := vars.Get("key")
	value := vars.Get("value")

	if key == "" || value == "" {
		h.log.Println("doSet() error, key or value is null")
		fmt.Fprint(w, "param error\n")
		return
	}

	event := payload{Op: "set", Key: key, Value: value}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		h.log.Printf("json.Marshal failed, err:%v", err)
		fmt.Fprint(w, "internal error\n")
		return
	}

	applyFuture := h.s.raftNode.raft.Apply(eventBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		h.log.Printf("raftNode.Apply failed:%v\n", err)
		fmt.Fprint(w, "internal error\n")
		return
	}

	fmt.Fprintf(w, "ok")
}

// doJoin handles joining cluster request
func (h *httpServer) doJoin(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()

	peerAddress := vars.Get("peerAddress")
	if peerAddress == "" {
		h.log.Println("invalid PeerAddress")
		fmt.Fprint(w, "invalid peerAddress\n")
		return
	}
	addPeerFuture := h.s.raftNode.raft.AddVoter(raft.ServerID(peerAddress), raft.ServerAddress(peerAddress), 0, 0)
	if err := addPeerFuture.Error(); err != nil {
		h.log.Printf("Error joining peer to raftNode, peeraddress:%s, err:%v, code:%d", peerAddress, err, http.StatusInternalServerError)
		fmt.Fprint(w, "internal error\n")
		return
	}
	fmt.Fprint(w, "ok")
}

func (h *httpServer) doDel(w http.ResponseWriter, r *http.Request) {
	if !h.checkWritePermission() {
		fmt.Fprint(w, WRITEABLE_ERR_MSG)
		return
	}
	vars := r.URL.Query()

	key := vars.Get("key")
	if key == "" {
		h.log.Println("doDel() error, key is null")
		fmt.Fprint(w, "param error\n")
		return
	}

	event := payload{Op: "delete", Key: key}
	eventBytes, err := json.Marshal(event)
	if err != nil {
		h.log.Printf("json.Marshal failed, err:%v", err)
		fmt.Fprint(w, "internal error\n")
		return
	}

	applyFuture := h.s.raftNode.raft.Apply(eventBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		h.log.Printf("raftNode.Apply failed:%v\n", err)
		fmt.Fprint(w, "internal error\n")
		return
	}

	fmt.Fprintf(w, "ok\n")
}

func (h *httpServer) doLeader(w http.ResponseWriter, r *http.Request) {
	leader := h.s.raftNode.raft.Leader()
	fmt.Fprintf(w, "%v\n", leader)
}

func (h *httpServer) doList(w http.ResponseWriter, r *http.Request) {
	//n := h.s.raftNode.raft.Stats()["latest_configuration"]
	n := h.s.raftNode.raft.GetConfiguration().Configuration().Servers
	fmt.Fprintf(w, "%v\n", n)
}
