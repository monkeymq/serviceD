package serviceD

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
)

type FSM serviceD

// Apply applies a Raft log entry to the key-value store.
func (f *FSM) Apply(l *raft.Log) interface{} {
	var p payload
	if err := json.Unmarshal(l.Data, &p); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch p.Op {
	case "set":
		return f.applySet(p.Key, p.Value)
	case "delete":
		return f.applyDelete(p.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", p.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]hostMapTime)
	f.m.Range(func(k, v interface{}) bool {
		o[k.(string)] = v.(hostMapTime)
		return true
	})
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *FSM) Restore(rc io.ReadCloser) error {
	o := make(map[string]hostMapTime)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	for k, v := range o {
		f.m.Store(k, v)
	}
	return nil
}

func (f *FSM) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logger.Printf("applySet. key: %v, value: %v \n", key, value)
	service, _ := f.m.LoadOrStore(key, make(hostMapTime))
	service.(hostMapTime)[value] = getNowInSencond()
	return nil
}

func (f *FSM) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m.Delete(key)
	return nil
}

type hostMapTime map[string]int64
type fsmSnapshot struct {
	store map[string]hostMapTime //serviceName => (ip:port => register time)
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		b, err := json.Marshal(f.store)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
