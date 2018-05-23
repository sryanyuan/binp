package storage

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/juju/errors"
)

// Signature for creating storage
const (
	LocalStorageSignature = "ls"
)

type localStorage struct {
	filename string
	kvs      map[string]interface{}
	mu       sync.Mutex
}

// NewLocalStorage create a local storage
func NewLocalStorage(filename string) (IStorage, error) {
	s := &localStorage{filename: filename,
		kvs: make(map[string]interface{})}
	err := s.loadLocal()
	if nil != err {
		return nil, errors.Trace(err)
	}
	return s, nil
}

func (s *localStorage) Get(key string) (interface{}, error) {
	s.mu.Lock()
	v, ok := s.kvs[key]
	s.mu.Unlock()

	if !ok {
		return nil, nil
	}
	return v, nil
}

func (s *localStorage) Set(key string, v interface{}) error {
	s.mu.Lock()
	s.kvs[key] = v
	s.mu.Unlock()
	return nil
}

func (s *localStorage) Delete(key string) error {
	s.mu.Lock()
	delete(s.kvs, key)
	s.mu.Unlock()
	return nil
}

func (s *localStorage) Save() error {
	s.mu.Lock()
	v, err := json.Marshal(s.kvs)
	if nil != err {
		return errors.Trace(err)
	}
	s.mu.Unlock()

	err = writeFileAtomic(s.filename, v, 0644)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

func (s *localStorage) loadLocal() error {
	data, err := ioutil.ReadFile(s.filename)
	if nil != err {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Trace(err)
	}
	err = json.Unmarshal(data, &s.kvs)
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

func writeFileAtomic(filename string, data []byte, perm os.FileMode) error {
	dir, name := path.Split(filename)
	f, err := ioutil.TempFile(dir, name)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	f.Close()
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	} else {
		err = os.Chmod(f.Name(), perm)
	}
	if err != nil {
		os.Remove(f.Name())
		return err
	}
	return os.Rename(f.Name(), filename)
}
