package commands

import "sync"

type telnetSessionRegistry struct {
	lock     sync.Mutex
	sessions map[string]map[*telnetClient]struct{}
}

func newTelnetSessionRegistry() *telnetSessionRegistry {
	return &telnetSessionRegistry{
		sessions: map[string]map[*telnetClient]struct{}{},
	}
}

func (r *telnetSessionRegistry) add(remoteIP string, c *telnetClient) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.sessions[remoteIP] == nil {
		r.sessions[remoteIP] = map[*telnetClient]struct{}{}
	}

	r.sessions[remoteIP][c] = struct{}{}
}

func (r *telnetSessionRegistry) remove(remoteIP string, c *telnetClient) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.sessions[remoteIP] == nil {
		return
	}

	delete(r.sessions[remoteIP], c)

	if len(r.sessions[remoteIP]) <= 0 {
		delete(r.sessions, remoteIP)
	}
}

func (r *telnetSessionRegistry) conflicts(remoteIP string, c *telnetClient) bool {
	r.lock.Lock()
	defer r.lock.Unlock()

	for session := range r.sessions[remoteIP] {
		if session == c {
			continue
		}

		return true
	}

	return false
}

func (r *telnetSessionRegistry) disconnectOthers(remoteIP string, c *telnetClient) {
	r.lock.Lock()
	targets := make([]*telnetClient, 0, len(r.sessions[remoteIP]))
	for session := range r.sessions[remoteIP] {
		if session == c {
			continue
		}

		targets = append(targets, session)
	}
	r.lock.Unlock()

	for i := range targets {
		targets[i].forceDisconnect()
	}
}

var telnetSessions = newTelnetSessionRegistry()
