// The C ABI adapts drive9's existing extent client. It introduces no server API.
package main

/*
#include <stdint.h>
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extent"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

type request struct {
	Op          string `json:"op"`
	Endpoint    string `json:"endpoint"`
	Token       string `json:"token"`
	Session     uint64 `json:"session"`
	Handle      uint64 `json:"handle"`
	Path        string `json:"path"`
	Target      string `json:"target"`
	Offset      uint64 `json:"offset"`
	Length      uint64 `json:"length"`
	Data        []byte `json:"data"`
	Write       bool   `json:"write"`
	Create      bool   `json:"create"`
	Exclusive   bool   `json:"exclusive"`
	Truncate    bool   `json:"truncate"`
	Lock        string `json:"lock"`
	HeartbeatMS int64  `json:"heartbeat_ms"`
}

type handle struct {
	ino   meta.Ino
	fh    uint64
	write bool
}
type session struct {
	mu       sync.RWMutex
	rt       *extent.Runtime
	files    map[uint64]handle
	failed   atomic.Bool
	deadline atomic.Pointer[time.Time]
	margin   time.Duration
	closed   bool // protected by mu, including calls that raced with disconnect
}

var sessions = struct {
	sync.Mutex
	next uint64
	all  map[uint64]*session
}{all: make(map[uint64]*session)}

func (s *session) healthy() error {
	// Fail closed before the confirmed session can expire. In
	// particular, a process resumed after SIGSTOP cannot revive an old lock.
	if d := s.deadline.Load(); d != nil && !time.Now().Before(*d) {
		s.failed.Store(true)
	}
	if s.failed.Load() {
		return errors.New("extent session failed or expired; close and reopen, do not replay writes")
	}
	return nil
}

func (s *session) setDeadline(expire int64) {
	// Retain Go's monotonic clock so a backward wall-clock adjustment cannot
	// extend an already confirmed session. Short heartbeats keep a full
	// heartbeat of margin; the default retains its five-second margin.
	now := time.Now()
	margin := s.margin
	if margin == 0 {
		margin = 5 * time.Second
	}
	deadline := now.Add(time.Unix(expire, 0).Add(-margin).Sub(now))
	s.deadline.Store(&deadline)
}

type metadataRPC func(context.Context, string, json.RawMessage) ([]byte, error)

func (s *session) metadata(ctx context.Context, op string, raw json.RawMessage, invoke metadataRPC) (json.RawMessage, int, error) {
	if err := s.healthy(); err != nil {
		return nil, int(syscall.EIO), err
	}
	deadline := time.Now().Add(30 * time.Second)
	if d := s.deadline.Load(); d != nil && d.Before(deadline) {
		deadline = *d
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	body, err := invoke(ctx, op, raw)
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = s.healthy()
	}
	if err != nil {
		// Cancellation cannot retract an already delivered RPC. Treat its
		// outcome as unknown; never acknowledge or replay it in this session.
		s.failed.Store(true)
		return nil, int(syscall.EIO), err
	}
	var result struct {
		Errno *int `json:"errno"`
	}
	if json.Unmarshal(body, &result) != nil || result.Errno == nil {
		s.failed.Store(true)
		return nil, int(syscall.EIO), errors.New("invalid extent metadata acknowledgement")
	}
	if op == "new_session" || op == "refresh_session" {
		var input struct {
			Expire int64 `json:"expire"`
		}
		if *result.Errno == 0 && json.Unmarshal(raw, &input) == nil {
			s.setDeadline(input.Expire)
		} else {
			s.failed.Store(true)
		}
		if err := s.healthy(); err != nil {
			return nil, int(syscall.EIO), err
		}
	}
	return body, 0, nil
}

func newSession(r request) (*session, error) {
	heartbeat := 12 * time.Second
	if r.HeartbeatMS != 0 {
		if r.HeartbeatMS < 1000 || r.HeartbeatMS > 600000 {
			return nil, errors.New("extent heartbeat_ms must be between 1000 and 600000")
		}
		heartbeat = time.Duration(r.HeartbeatMS) * time.Millisecond
	}
	s := &session{files: make(map[uint64]handle), margin: min(5*time.Second, heartbeat)}
	c := client.New(r.Endpoint, r.Token)
	ctx, cancel := context.WithTimeout(context.Background(), extent.CredentialMintTimeout)
	defer cancel()
	cred, err := c.GetDataCredential(ctx)
	if err != nil {
		return nil, fmt.Errorf("extent credential: %w", err)
	}
	storage, err := extent.OpenStorage(cred, c)
	if err != nil {
		return nil, err
	}
	transport := extent.NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		return s.metadata(ctx, op, raw, func(ctx context.Context, op string, raw json.RawMessage) ([]byte, error) {
			return c.ExtentMeta(ctx, op, raw)
		})
	})
	s.rt, err = extent.NewRuntime(extent.RuntimeConfig{Transport: transport, Storage: storage, CacheBytes: 64 << 20,
		Heartbeat: heartbeat})
	if err != nil {
		return nil, err
	}
	if heartbeat < 12*time.Second && !transport.SessionGuardSupported() {
		_ = extent.CloseRuntime(s.rt)
		return nil, errors.New("short extent heartbeats require a server advertising session_guard")
	}
	return s, nil
}

func fileContext(path string) vfs.LogContext {
	return vfs.NewLogContext(meta.NewContext(uint32(os.Getpid()), 0, []uint32{0}).WithValue(meta.Drive9PathKey, path))
}

func canonical(p string) (string, error) {
	if p == "" || !strings.HasPrefix(p, "/") {
		return "", syscall.EINVAL
	}
	p, err := pathutil.Canonicalize(p)
	if err != nil {
		return "", err
	}
	if p != "/" {
		p = strings.TrimSuffix(p, "/")
	}
	return p, nil
}

func (s *session) lookup(p string) (*meta.Entry, syscall.Errno) {
	ctx := fileContext(p)
	entry, eno := s.rt.VFS.GetAttr(ctx, 1, 0)
	if eno != 0 || p == "/" {
		return entry, eno
	}
	for _, name := range strings.Split(strings.TrimPrefix(p, "/"), "/") {
		entry, eno = s.rt.VFS.Lookup(ctx, entry.Inode, name)
		if eno != 0 {
			return nil, eno
		}
	}
	return entry, 0
}

func (s *session) parent(p string) (meta.Ino, string, syscall.Errno) {
	if p == "/" {
		return 0, "", syscall.EINVAL
	}
	i := strings.LastIndexByte(p, '/')
	dir := p[:i]
	if dir == "" {
		dir = "/"
	}
	e, eno := s.lookup(dir)
	if eno != 0 {
		return 0, "", eno
	}
	if e.Attr.Typ != meta.TypeDirectory {
		return 0, "", syscall.ENOTDIR
	}
	return e.Inode, p[i+1:], 0
}

func info(e *meta.Entry) map[string]any {
	return map[string]any{"exists": true, "directory": e.Attr.Typ == meta.TypeDirectory, "size": e.Attr.Length, "inode": e.Inode}
}

// Readers share a lifetime guard. All namespace/handle mutations and runtime
// shutdown retain the exclusive guard, so no VFS read can outlive its handle.
func (s *session) withReadHandle(id uint64, read func(handle) (int, error)) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, syscall.EBADF
	}
	if err := s.healthy(); err != nil {
		return 0, err
	}
	h, ok := s.files[id]
	if !ok {
		return 0, syscall.EBADF
	}
	n, err := read(h)
	if err != nil {
		return 0, err
	}
	if err := s.healthy(); err != nil {
		return 0, err
	}
	return n, nil
}

const maxRead = 1 << 20

func validRead(length, offset uint64) bool {
	return length <= maxRead && offset <= math.MaxInt64 && length <= math.MaxInt64-offset
}

// buffer belongs to the synchronous caller. VFS.Read fills it before returning;
// neither the bridge nor VFS retains the buffer after this call.
func (s *session) readInto(id uint64, buffer []byte, offset uint64) (int, error) {
	if !validRead(uint64(len(buffer)), offset) {
		return 0, syscall.EINVAL
	}
	return s.withReadHandle(id, func(h handle) (int, error) {
		n, eno := s.rt.VFS.Read(fileContext(""), h.ino, buffer, offset, h.fh)
		if eno != 0 {
			return 0, eno
		}
		if n < 0 || n > len(buffer) {
			return 0, syscall.EIO
		}
		return n, nil
	})
}

func (s *session) call(r request) (map[string]any, error) {
	if r.Op == "read" {
		if !validRead(r.Length, r.Offset) {
			return nil, syscall.EINVAL
		}
		data := make([]byte, int(r.Length))
		n, err := s.readInto(r.Handle, data, r.Offset)
		return map[string]any{"data": data[:n]}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, syscall.EBADF
	}
	if err := s.healthy(); err != nil {
		return nil, err
	}
	p, err := canonical(r.Path)
	if r.Path != "" && err != nil {
		return nil, err
	}
	ctx := fileContext(p)
	v := s.rt.VFS
	check := func(eno syscall.Errno) error {
		if eno != 0 {
			return eno
		}
		return nil
	}
	switch r.Op {
	case "stat":
		e, eno := s.lookup(p)
		if eno == syscall.ENOENT {
			return map[string]any{"exists": false, "directory": false}, nil
		}
		if eno != 0 {
			return nil, eno
		}
		return info(e), nil
	case "open":
		parent, name, eno := s.parent(p)
		if eno != 0 {
			return nil, eno
		}
		e, eno := s.lookup(p)
		flags := uint32(syscall.O_RDONLY)
		if r.Write {
			flags = syscall.O_RDWR
		}
		var fh uint64
		if eno == syscall.ENOENT && r.Create {
			e, fh, eno = v.Create(ctx, parent, name, 0600, 0, flags|syscall.O_CREAT|syscall.O_EXCL)
			if eno == syscall.EEXIST && !r.Exclusive {
				e, eno = s.lookup(p)
				if eno == 0 {
					e, fh, eno = v.Open(ctx, e.Inode, flags)
				}
			}
		} else if eno == 0 {
			if r.Exclusive {
				return nil, syscall.EEXIST
			}
			if e.Attr.Typ != meta.TypeFile {
				return nil, syscall.EISDIR
			}
			e, fh, eno = v.Open(ctx, e.Inode, flags)
		}
		if eno != 0 {
			return nil, eno
		}
		ok := false
		defer func() {
			if !ok {
				v.Release(ctx, e.Inode, fh)
			}
		}()
		if r.Lock != "" {
			typ := uint32(syscall.F_RDLCK)
			if r.Lock == "write" {
				typ = syscall.F_WRLCK
			}
			// Whole-file POSIX record lock, identical to DuckDB's local backend.
			if eno = v.Setlk(ctx, e.Inode, fh, fh, 0, math.MaxInt64, typ, uint32(os.Getpid()), false); eno != 0 {
				return nil, fmt.Errorf("file is locked: %w", eno)
			}
			// A failed lock attempt can warm the SDK's one-second open cache
			// before the previous owner checkpoints or changes the file. Once
			// ownership is acquired, discard that snapshot and update VFS reader
			// lengths from authoritative attributes (including same-size edits).
			if eno = v.Meta.InvalidateChunkCache(ctx, e.Inode, math.MaxUint32); eno != 0 {
				return nil, eno
			}
			fresh, eno := v.GetAttr(ctx, e.Inode, 0)
			if eno != 0 {
				return nil, eno
			}
			e = fresh
		}
		if r.Truncate {
			if !r.Write {
				return nil, syscall.EACCES
			}
			if _, eno = v.SetAttr(ctx, e.Inode, int(meta.SetAttrSize), fh, 0, 0, 0, 0, 0, 0, 0, 0); eno != 0 {
				return nil, eno
			}
			e.Attr.Length = 0
		}
		s.files[fh] = handle{e.Inode, fh, r.Write}
		ok = true
		out := info(e)
		out["handle"] = fh
		return out, nil
	case "mkdir", "remove":
		parent, name, eno := s.parent(p)
		if eno != 0 {
			return nil, eno
		}
		if r.Op == "mkdir" {
			_, eno = v.Mkdir(ctx, parent, name, 0700, 0)
			return map[string]any{}, check(eno)
		}
		e, eno := s.lookup(p)
		if eno != 0 {
			return nil, eno
		}
		if e.Attr.Typ == meta.TypeDirectory {
			eno = v.Rmdir(ctx, parent, name)
		} else {
			eno = v.Unlink(ctx, parent, name)
		}
		return map[string]any{}, check(eno)
	case "rename":
		target, err := canonical(r.Target)
		if err != nil {
			return nil, err
		}
		parent, name, eno := s.parent(p)
		if eno != 0 {
			return nil, eno
		}
		tparent, tname, eno := s.parent(target)
		if eno != 0 {
			return nil, eno
		}
		return map[string]any{}, check(v.Rename(fileContext(target), parent, name, tparent, tname, 0))
	case "list":
		e, eno := s.lookup(p)
		if eno != 0 {
			return nil, eno
		}
		fh, eno := v.Opendir(ctx, e.Inode, 0)
		if eno != 0 {
			return nil, eno
		}
		defer v.Releasedir(ctx, e.Inode, fh)
		entries := []map[string]any{}
		for off := 0; ; {
			batch, _, eno := v.Readdir(ctx, e.Inode, 4096, off, fh, true)
			if eno != 0 {
				return nil, eno
			}
			if len(batch) == 0 {
				break
			}
			for _, child := range batch {
				name := string(child.Name)
				if name != "." && name != ".." {
					entries = append(entries, map[string]any{"name": name, "directory": child.Attr.Typ == meta.TypeDirectory})
				}
			}
			off += len(batch)
			v.UpdateReaddirOffset(ctx, e.Inode, fh, off)
		}
		return map[string]any{"entries": entries}, nil
	}
	h, ok := s.files[r.Handle]
	if !ok {
		return nil, syscall.EBADF
	}
	if r.Offset > math.MaxInt64 || r.Length > 1<<20 || len(r.Data) > 1<<20 {
		return nil, syscall.EINVAL
	}
	switch r.Op {
	case "write":
		if !h.write {
			return nil, syscall.EACCES
		}
		if eno := v.Write(ctx, h.ino, r.Data, r.Offset, h.fh); eno != 0 {
			s.failed.Store(true)
			return nil, eno
		}
		// Conservative initial policy: acknowledge only after existing VFS has
		// uploaded blocks and published slice metadata. No dirty close errors.
		if eno := v.Fsync(ctx, h.ino, 0, h.fh); eno != 0 {
			s.failed.Store(true)
			return nil, eno
		}
	case "sync":
		if eno := v.Fsync(ctx, h.ino, 0, h.fh); eno != 0 {
			s.failed.Store(true)
			return nil, eno
		}
	case "truncate":
		if !h.write {
			return nil, syscall.EACCES
		}
		_, eno := v.SetAttr(ctx, h.ino, int(meta.SetAttrSize), h.fh, 0, 0, 0, 0, 0, 0, 0, r.Offset)
		if eno != 0 {
			s.failed.Store(true)
			return nil, eno
		}
	case "close":
		if eno := v.Flush(ctx, h.ino, h.fh, h.fh); eno != 0 {
			s.failed.Store(true)
			return nil, eno
		}
		v.Release(ctx, h.ino, h.fh)
		delete(s.files, r.Handle)
	default:
		return nil, syscall.ENOSYS
	}
	return map[string]any{}, s.healthy()
}

func dispatch(r request) (map[string]any, error) {
	if r.Op == "connect" {
		s, err := newSession(r)
		if err != nil {
			return nil, err
		}
		sessions.Lock()
		defer sessions.Unlock()
		sessions.next++
		sessions.all[sessions.next] = s
		return map[string]any{"session": sessions.next}, nil
	}
	sessions.Lock()
	s := sessions.all[r.Session]
	if r.Op == "disconnect" {
		delete(sessions.all, r.Session)
	}
	sessions.Unlock()
	if s == nil {
		return nil, syscall.EBADF
	}
	if r.Op == "disconnect" {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.closed = true
		for _, h := range s.files {
			s.rt.VFS.Release(fileContext(""), h.ino, h.fh)
		}
		return map[string]any{}, extent.CloseRuntime(s.rt)
	}
	return s.call(r)
}

func readBuffer(sid, id uint64, buffer unsafe.Pointer, length, offset uint64) (int, error) {
	if !validRead(length, offset) || (length != 0 && buffer == nil) {
		return 0, syscall.EINVAL
	}
	sessions.Lock()
	s := sessions.all[sid]
	sessions.Unlock()
	if s == nil {
		return 0, syscall.EBADF
	}
	return s.readInto(id, unsafe.Slice((*byte)(buffer), int(length)), offset)
}

//export tidbfs_extent_read
func tidbfs_extent_read(sid, id C.uint64_t, buffer unsafe.Pointer, length, offset C.uint64_t, count *C.uint64_t) (failure *C.char) {
	// Only errors allocate C memory. Success has no JSON, Base64, or bridge
	// payload allocation/copy. The C++ caller frees errors with extent_free.
	defer func() {
		if recover() != nil {
			if count != nil {
				*count = 0
			}
			failure = C.CString("extent read panic")
		}
	}()
	if count == nil {
		return C.CString("missing extent read count")
	}
	*count = 0
	n, err := readBuffer(uint64(sid), uint64(id), buffer, uint64(length), uint64(offset))
	if err != nil {
		return C.CString(err.Error())
	}
	*count = C.uint64_t(n)
	return nil
}

//export tidbfs_extent_call
func tidbfs_extent_call(raw *C.char) (out *C.char) {
	defer func() {
		if recover() != nil {
			out = C.CString(`{"error":"extent bridge panic"}`)
		}
	}()
	var r request
	err := json.Unmarshal([]byte(C.GoString(raw)), &r)
	var result map[string]any
	if err == nil {
		result, err = dispatch(r)
	}
	if err != nil {
		result = map[string]any{"error": err.Error()}
	}
	data, _ := json.Marshal(result)
	return C.CString(string(data))
}

//export tidbfs_extent_free
func tidbfs_extent_free(p *C.char) { C.free(unsafe.Pointer(p)) }

func main() {}
