// Package modal implements an rclone backend for Modal Volumes.
package modal

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/list"
	"github.com/rclone/rclone/lib/encoder"
)

//go:embed volume.py
var volumeScript string

var (
	errPathNotFound = errors.New("volume path not found")
	errV1Directory  = errors.New("Volume v1 cannot remove a directory non-recursively")
	errConflict     = errors.New("volume path conflict")
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "modal",
		Description: "Modal Volume",
		NewFs:       NewFs,
		Options: fs.Options{{
			Name: "volume",
			Help: "Name of an existing Modal Volume.",
		}, {
			Name:     "volume_id",
			Help:     "ID of an existing Volume. Use this in place of volume.",
			Advanced: true,
		}, {
			Name: "environment",
			Help: "Modal environment containing the named Volume. Leave empty to use Modal's default.",
		}, {
			Name:     "python",
			Help:     "Python executable with the Modal SDK installed.",
			Default:  "python3",
			Advanced: true,
		}, {
			Name:     "encoding",
			Help:     config.ConfigEncodingHelp,
			Default:  encoder.Display | encoder.EncodeInvalidUtf8,
			Advanced: true,
		}},
	})
}

// Options defines the configuration for a Modal Volume remote.
type Options struct {
	Volume      string               `config:"volume"`
	VolumeID    string               `config:"volume_id"`
	Environment string               `config:"environment"`
	Python      string               `config:"python"`
	Enc         encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a directory in one Modal Volume.
type Fs struct {
	name     string
	root     string
	opt      Options
	volumeID string
	features *fs.Features
}

type entry struct {
	Path     string `json:"path"`
	Type     string `json:"type"`
	Size     int64  `json:"size"`
	ModTime  int64  `json:"mtime"`
	Nonempty bool   `json:"nonempty"`
}

// Object represents a file in a Modal Volume.
type Object struct {
	fs     *Fs
	remote string
	size   int64
	mtime  time.Time
}

// NewFs constructs an Fs from a named Volume or Volume ID and a relative root.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	f := &Fs{name: name, root: strings.TrimPrefix(path.Clean("/"+root), "/")}
	if err := configstruct.Set(m, &f.opt); err != nil {
		return nil, err
	}
	if (f.opt.Volume == "") == (f.opt.VolumeID == "") {
		return nil, errors.New("configure exactly one of volume and volume_id")
	}
	f.features = (&fs.Features{NoMultiThreading: true}).Fill(ctx, f)
	f.volumeID = f.opt.VolumeID
	reader, err := f.start(ctx, "resolve", "/")
	if err != nil {
		return nil, err
	}
	err = decodeResult(reader, &f.volumeID)
	if err != nil {
		return nil, fmt.Errorf("resolve Modal Volume: %w", err)
	}

	if f.root != "" {
		info, err := f.stat(ctx, "")
		if err != nil && !errors.Is(err, errPathNotFound) {
			return nil, err
		}
		if err == nil && info.Type != "DIRECTORY" {
			f.root = strings.TrimPrefix(path.Dir("/"+f.root), "/")
			return f, fs.ErrorIsFile
		}
	}
	return f, nil
}

func (f *Fs) absolute(remote string) string {
	relative := strings.TrimPrefix(path.Clean("/"+remote), "/")
	return "/" + f.opt.Enc.FromStandardPath(path.Join(f.root, relative))
}

type processReader struct {
	*io.PipeReader
	cancel context.CancelFunc
	done   <-chan struct{}
}

func (r *processReader) Close() error {
	r.cancel()
	err := r.PipeReader.Close()
	<-r.done
	return err
}

func (f *Fs) start(ctx context.Context, operation, remote string, args ...string) (*processReader, error) {
	childCtx, cancel := context.WithCancel(ctx)
	argv := []string{"-P", "-u", "-c", volumeScript, operation, f.opt.Volume, f.volumeID, f.opt.Environment, remote}
	cmd := exec.CommandContext(childCtx, f.opt.Python, append(argv, args...)...)
	var stderr bytes.Buffer
	input, output := io.Pipe()
	cmd.Stdout, cmd.Stderr = output, &stderr
	if err := cmd.Start(); err != nil {
		cancel()
		_ = output.Close()
		_ = input.Close()
		return nil, fmt.Errorf("start Modal SDK using %q: %w", f.opt.Python, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		err := cmd.Wait()
		if ctx.Err() != nil {
			err = ctx.Err()
		} else if err != nil {
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				switch exit.ExitCode() {
				case 20:
					err = errPathNotFound
				case 21:
					err = errV1Directory
				case 22:
					err = errConflict
				}
			}
			err = fmt.Errorf("Modal %s %q: %w: %s", operation, remote, err, strings.TrimSpace(stderr.String()))
		}
		_ = output.CloseWithError(err)
	}()
	return &processReader{PipeReader: input, cancel: cancel, done: done}, nil
}

func decodeResult(reader *processReader, value any) (err error) {
	defer fs.CheckClose(reader, &err)
	decoder := json.NewDecoder(reader)
	if err = decoder.Decode(value); err != nil {
		return err
	}
	// Read the process's final status even when it emitted valid JSON.
	_, err = io.Copy(io.Discard, reader)
	return err
}

func (f *Fs) stat(ctx context.Context, remote string) (info entry, err error) {
	reader, err := f.start(ctx, "stat", f.absolute(remote))
	if err != nil {
		return info, err
	}
	err = decodeResult(reader, &info)
	return info, err
}

// Name returns the configured remote name.
func (f *Fs) Name() string { return f.name }

// Root returns the directory root relative to the Volume.
func (f *Fs) Root() string { return f.root }

// String describes this Modal Volume remote.
func (f *Fs) String() string {
	return fmt.Sprintf("Modal Volume %s path %q", f.volumeID, f.root)
}

// Precision returns the resolution of timestamps reported by the Volume.
func (f *Fs) Precision() time.Duration { return time.Second }

// Hashes returns the set of checksums available without downloading a file.
func (f *Fs) Hashes() hash.Set { return hash.Set(0) }

// Features returns the optional capabilities of the Fs.
func (f *Fs) Features() *fs.Features { return f.features }

func (f *Fs) list(ctx context.Context, dir string, recursive bool, add func(fs.DirEntry) error) (err error) {
	operation := "list"
	if recursive {
		operation = "list-recursive"
	}
	absolute := f.absolute(dir)
	reader, err := f.start(ctx, operation, absolute)
	if err != nil {
		return err
	}
	defer fs.CheckClose(reader, &err)
	decoder := json.NewDecoder(reader)
	for {
		var info entry
		err = decoder.Decode(&info)
		if err == io.EOF {
			return nil
		}
		if errors.Is(err, errPathNotFound) {
			return fs.ErrorDirNotFound
		}
		if err != nil {
			return err
		}
		listed := "/" + strings.TrimLeft(info.Path, "/")
		if listed == absolute {
			return fs.ErrorDirNotFound
		}
		if !strings.HasPrefix(listed, strings.TrimRight(absolute, "/")+"/") || path.Clean(listed) != listed {
			return fmt.Errorf("Modal returned path %q outside directory %q", listed, absolute)
		}
		remote := f.opt.Enc.ToStandardPath(strings.TrimPrefix(listed, strings.TrimRight(f.absolute(""), "/")+"/"))
		switch info.Type {
		case "DIRECTORY":
			err = add(fs.NewDir(remote, time.Unix(info.ModTime, 0)))
		case "FILE":
			err = add(f.newObject(remote, info))
		default:
			fs.Debugf(f, "Skipping %q with Modal file type %s", remote, info.Type)
		}
		if err != nil {
			return err
		}
	}
}

// List returns the files and subdirectories directly inside dir.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	err = f.list(ctx, dir, false, func(entry fs.DirEntry) error {
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

// ListR recursively lists files and subdirectories inside dir.
func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	helper := list.NewHelper(callback)
	if err := f.list(ctx, dir, true, helper.Add); err != nil {
		return err
	}
	return helper.Flush()
}

func (f *Fs) newObject(remote string, info entry) *Object {
	return &Object{fs: f, remote: remote, size: info.Size, mtime: time.Unix(info.ModTime, 0)}
}

// NewObject looks up a regular file by remote path.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	info, err := f.stat(ctx, remote)
	if errors.Is(err, errPathNotFound) {
		return nil, fs.ErrorObjectNotFound
	}
	if err != nil {
		return nil, err
	}
	switch info.Type {
	case "FILE":
		return f.newObject(remote, info), nil
	case "DIRECTORY":
		return nil, fs.ErrorIsDir
	default:
		return nil, fs.ErrorNotAFile
	}
}

// Put writes a regular file, creating its parent directories as needed.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	o := &Object{fs: f, remote: src.Remote()}
	if err := o.Update(ctx, in, src, options...); err != nil {
		return nil, err
	}
	return o, nil
}

// PutStream writes a regular file whose size may be unknown.
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

// Mkdir succeeds for a directory whose creation is deferred until the first upload.
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return nil
}

// Rmdir removes an empty directory without recursively deleting its contents.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	entry, err := f.stat(ctx, dir)
	if errors.Is(err, errPathNotFound) {
		return fs.ErrorDirNotFound
	}
	if err != nil {
		return err
	}
	if entry.Type != "DIRECTORY" {
		return fs.ErrorDirNotFound
	}
	if entry.Nonempty {
		return fs.ErrorDirectoryNotEmpty
	}
	if f.absolute(dir) == "/" {
		return nil
	}
	err = f.run(ctx, "remove", f.absolute(dir))
	switch {
	case errors.Is(err, errPathNotFound):
		return fs.ErrorDirNotFound
	case errors.Is(err, errConflict):
		return fs.ErrorDirectoryNotEmpty
	default:
		return err
	}
}

func (f *Fs) run(ctx context.Context, operation, remote string, args ...string) (err error) {
	reader, err := f.start(ctx, operation, remote, args...)
	if err != nil {
		return err
	}
	defer fs.CheckClose(reader, &err)
	_, err = io.Copy(io.Discard, reader)
	return err
}

// Fs returns the filesystem containing the object.
func (o *Object) Fs() fs.Info { return o.fs }

// String returns the remote path of the object.
func (o *Object) String() string { return o.remote }

// Remote returns the object's path relative to the Fs root.
func (o *Object) Remote() string { return o.remote }

// ModTime returns the last modification time reported by Modal.
func (o *Object) ModTime(ctx context.Context) time.Time { return o.mtime }

// Size returns the object's size in bytes.
func (o *Object) Size() int64 { return o.size }

// Storable reports whether the object can be copied.
func (o *Object) Storable() bool { return true }

// Hash reports that Modal does not expose whole-file checksums.
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// SetModTime reports that the Modal API cannot set the object's modification time.
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Open reads the file or the requested byte range.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	var offset int64
	limit := int64(-1)
	for _, option := range options {
		switch option := option.(type) {
		case *fs.RangeOption:
			offset, limit = option.Decode(o.size)
		case *fs.SeekOption:
			offset, limit = option.Offset, -1
		default:
			if option.Mandatory() {
				return nil, fmt.Errorf("unsupported open option %v", option)
			}
		}
	}
	if offset < 0 {
		return nil, fmt.Errorf("invalid read offset %d", offset)
	}
	if (offset > 0 && offset >= o.size) || limit == 0 {
		return io.NopCloser(strings.NewReader("")), nil
	}
	reader, err := o.fs.start(ctx, "read", o.fs.absolute(o.remote), strconv.FormatInt(offset, 10), strconv.FormatInt(limit, 10))
	if err != nil {
		return nil, err
	}
	buffer := bufio.NewReader(reader)
	if _, err = buffer.Peek(1); err != nil && err != io.EOF {
		_ = reader.Close()
		if errors.Is(err, errPathNotFound) {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	return struct {
		io.Reader
		io.Closer
	}{Reader: buffer, Closer: reader}, nil
}

// Update overwrites the file with the supplied contents.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	temp, err := os.CreateTemp("", "rclone-modal-")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temp.Name()) }()
	size, err := io.Copy(temp, in)
	closeErr := temp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if src.Size() >= 0 && size != src.Size() {
		return fmt.Errorf("upload size changed from %d to %d bytes", src.Size(), size)
	}
	if err := o.fs.run(ctx, "put", o.fs.absolute(o.remote), temp.Name()); err != nil {
		return err
	}
	o.size = size
	o.mtime = time.Now().Truncate(time.Second)
	return nil
}

// Remove deletes the file from the Volume.
func (o *Object) Remove(ctx context.Context) error {
	err := o.fs.run(ctx, "remove", o.fs.absolute(o.remote))
	if errors.Is(err, errPathNotFound) {
		return fs.ErrorObjectNotFound
	}
	return err
}

var (
	_ fs.Fs          = (*Fs)(nil)
	_ fs.ListRer     = (*Fs)(nil)
	_ fs.PutStreamer = (*Fs)(nil)
	_ fs.Object      = (*Object)(nil)
)
