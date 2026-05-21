// Sshwifty - A Web SSH client
//
// Copyright (C) 2019-2022 Ni Rui <ranqus@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package commands

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/nirui/sshwifty/application/command"
	"github.com/nirui/sshwifty/application/configuration"
	"github.com/nirui/sshwifty/application/log"
	"github.com/nirui/sshwifty/application/network"
	"github.com/nirui/sshwifty/application/rw"
)

// Errors
var (
	ErrTelnetUnableToReceiveRemoteConn = errors.New(
		"unable to acquire remote connection handle")
)

// Error codes
const (
	TelnetRequestErrorBadRemoteAddress = command.StreamError(0x01)
)

const (
	telnetDefaultPortString = "23"
)

// Server signal codes
const (
	TelnetServerRemoteBand    = 0x00
	TelnetServerDialFailed    = 0x01
	TelnetServerDialConnected = 0x02
	TelnetServerConflict      = 0x03
)

const (
	TelnetClientData    = 0x00
	TelnetClientConfirm = 0x01
)

type telnetClient struct {
	l          log.Logger
	w          command.StreamResponder
	cfg        command.Configuration
	remoteChan chan net.Conn
	remoteConn net.Conn
	closeWait  sync.WaitGroup
	remoteIP   string
	decision   chan bool
	acquired   bool
	stateLock  sync.Mutex
}

func newTelnet(
	l log.Logger,
	w command.StreamResponder,
	cfg command.Configuration,
) command.FSMMachine {
	return &telnetClient{
		l:          l,
		w:          w,
		cfg:        cfg,
		remoteChan: make(chan net.Conn, 1),
		remoteConn: nil,
		closeWait:  sync.WaitGroup{},
		remoteIP:   "",
		decision:   make(chan bool, 1),
		acquired:   false,
		stateLock:  sync.Mutex{},
	}
}

func parseTelnetConfig(p configuration.Preset) (configuration.Preset, error) {
	oldHost := p.Host

	_, _, sErr := net.SplitHostPort(p.Host)

	if sErr != nil {
		p.Host = net.JoinHostPort(p.Host, telnetDefaultPortString)
	}

	if len(p.Host) <= 0 {
		p.Host = oldHost
	}

	return p, nil
}

func (d *telnetClient) Bootup(
	r *rw.LimitedReader,
	b []byte) (command.FSMState, command.FSMError) {
	addr, addrErr := ParseAddress(r.Read, b)

	if addrErr != nil {
		return nil, command.ToFSMError(
			addrErr, TelnetRequestErrorBadRemoteAddress)
	}

	host, _, splitErr := net.SplitHostPort(addr.String())
	if splitErr != nil {
		return nil, command.ToFSMError(
			splitErr, TelnetRequestErrorBadRemoteAddress)
	}

	d.remoteIP = host

	d.closeWait.Add(1)
	go d.remote(addr.String())

	return d.client, command.NoFSMError()
}

func (d *telnetClient) acquireOrConfirm(buf []byte) bool {
	telnetSessions.add(d.remoteIP, d)

	if !telnetSessions.conflicts(d.remoteIP, d) {
		d.acquired = true
		return true
	}

	msgLen := copy(buf[d.w.HeaderSize():], d.remoteIP)
	if msgLen <= 0 {
		msgLen = copy(buf[d.w.HeaderSize():], "remote")
	}

	_ = d.w.SendManual(TelnetServerConflict, buf[:msgLen+d.w.HeaderSize()])

	for {
		approved, ok := <-d.decision
		if !ok {
			return false
		}

		if !approved {
			return false
		}

		telnetSessions.disconnectOthers(d.remoteIP, d)

		for telnetSessions.conflicts(d.remoteIP, d) {
			time.Sleep(10 * time.Millisecond)
		}

		d.acquired = true
		return true
	}
}

func (d *telnetClient) remote(addr string) {
	defer func() {
		d.w.Signal(command.HeaderClose)

		telnetSessions.remove(d.remoteIP, d)
		close(d.remoteChan)
		d.closeWait.Done()
	}()

	buf := [4096]byte{}

	if !d.acquireOrConfirm(buf[:]) {
		errLen := copy(
			buf[d.w.HeaderSize():], "Connection cancelled") + d.w.HeaderSize()
		d.w.SendManual(TelnetServerDialFailed, buf[:errLen])
		return
	}

	clientConn, clientConnErr := d.cfg.Dial("udp", addr, d.cfg.DialTimeout)

	if clientConnErr != nil {
		errLen := copy(
			buf[d.w.HeaderSize():], clientConnErr.Error()) + d.w.HeaderSize()
		d.w.SendManual(TelnetServerDialFailed, buf[:errLen])

		return
	}

	defer clientConn.Close()

	clientConnErr = d.w.SendManual(
		TelnetServerDialConnected,
		buf[:d.w.HeaderSize()],
	)

	if clientConnErr != nil {
		return
	}

	clientConn.SetWriteDeadline(time.Now().Add(d.cfg.DialTimeout))
	timeoutClientConn := network.NewWriteTimeoutConn(
		clientConn, d.cfg.DialTimeout)

	d.stateLock.Lock()
	d.remoteConn = &timeoutClientConn
	d.stateLock.Unlock()
	d.remoteChan <- &timeoutClientConn

	for {
		rLen, rErr := clientConn.Read(buf[d.w.HeaderSize():])

		if rErr != nil {
			return
		}

		wErr := d.w.SendManual(
			TelnetServerRemoteBand, buf[:rLen+d.w.HeaderSize()])

		if wErr != nil {
			return
		}
	}
}

func (d *telnetClient) forceDisconnect() {
	d.stateLock.Lock()
	defer d.stateLock.Unlock()

	if d.remoteConn != nil {
		d.remoteConn.Close()
	}
}

func (d *telnetClient) getRemote() (net.Conn, error) {
	if d.remoteConn != nil {
		return d.remoteConn, nil
	}

	remoteConn, ok := <-d.remoteChan

	if !ok {
		return nil, ErrTelnetUnableToReceiveRemoteConn
	}

	d.remoteConn = remoteConn

	return d.remoteConn, nil
}

func (d *telnetClient) client(
	f *command.FSM,
	r *rw.LimitedReader,
	h command.StreamHeader,
	b []byte,
) error {
	if h.Marker() == TelnetClientConfirm {
		confirmData, confirmErr := rw.FetchOneByte(r.Fetch)
		if confirmErr != nil {
			return confirmErr
		}

		d.decision <- (confirmData[0] == 1)
		return nil
	}

	remoteConn, remoteConnErr := d.getRemote()

	if remoteConnErr != nil {
		return remoteConnErr
	}

	for !r.Completed() {
		rBuf, rErr := r.Buffered()

		if rErr != nil {
			return rErr
		}

	var wErr error
		if len(rBuf) == 1 && rBuf[0] == 127{
			rBuf[0] = 8
			_, wErr = remoteConn.Write(rBuf)
		} else {
		_, wErr = remoteConn.Write(rBuf)
		}

		if wErr != nil {
			remoteConn.Close()

			d.l.Debug("Failed to write data to remote: %s", wErr)
		}
	}

	return nil
}

func (d *telnetClient) Close() error {
	d.stateLock.Lock()
	acquired := d.acquired
	remoteConn := d.remoteConn
	d.stateLock.Unlock()

	if !acquired {
		select {
		case d.decision <- false:
		default:
		}

		d.closeWait.Wait()
		return nil
	}

	if remoteConn != nil {
		remoteConn.Close()
		d.closeWait.Wait()
		return nil
	}

	remoteConn, remoteConnErr := d.getRemote()

	if remoteConnErr == nil {
		remoteConn.Close()
	}

	d.closeWait.Wait()

	return nil
}

func (d *telnetClient) Release() error {
	return nil
}
