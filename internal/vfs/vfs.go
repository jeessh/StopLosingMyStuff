package vfs

import (
	"io"
	"os"
	"path/filepath"
	"sort"
)

type File interface {
	io.Writer
	Sync() error
	Truncate(size int64) error
	Close() error
}

type FS interface {
	MkdirAll(dir string) error
	Append(path string) (File, error)
	Create(path string) (File, error)
	ReadFile(path string) ([]byte, error)
	Rename(from, to string) error
	Remove(path string) error
	List(dir string) ([]string, error)
	SyncDir(dir string) error
}

func WriteAtomic(fsys FS, path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := fsys.Create(tmp)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = fsys.Remove(tmp)
		return err
	}
	if err := fsys.Rename(tmp, path); err != nil {
		return err
	}
	return fsys.SyncDir(filepath.Dir(path))
}

type OS struct{}

func (OS) MkdirAll(dir string) error { return os.MkdirAll(dir, 0o700) }

func (OS) Append(path string) (File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
}

func (OS) Create(path string) (File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
}

func (OS) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }
func (OS) Rename(from, to string) error         { return os.Rename(from, to) }
func (OS) Remove(path string) error             { return os.Remove(path) }

func (OS) List(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func (OS) SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
