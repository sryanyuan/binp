package main

import (
	"encoding/json"
	"time"

	"github.com/juju/errors"
	"github.com/sryanyuan/binp/mconn"
	"github.com/sryanyuan/binp/storage"
)

const (
	storageKeyPosition = "position"
	lazySaveTime       = 30
)

var (
	errStorageKeyNotFound = errors.New("Storage key not found")
)

// storageReaderWriter wrap the storage interface and expose interface to read with specified key
type storageReaderWriter struct {
	st           storage.IStorage
	lastSaveTime int64
}

func newStorageReaderWriter(st storage.IStorage) *storageReaderWriter {
	return &storageReaderWriter{
		st:           st,
		lastSaveTime: time.Now().Unix(),
	}
}

func (r *storageReaderWriter) readPosition(pos *mconn.ReplicationPoint) error {
	v, err := r.st.Get(storageKeyPosition)
	if nil != err {
		return errors.Trace(err)
	}
	if nil == v && nil == err {
		return errStorageKeyNotFound
	}
	sv, ok := v.(string)
	if !ok {
		return errors.New("Invalid format")
	}
	if err := json.Unmarshal([]byte(sv), pos); nil != err {
		return errors.Trace(err)
	}
	return nil
}

func (r *storageReaderWriter) writePosition(pos *mconn.ReplicationPoint) error {
	v, err := json.Marshal(pos)
	if nil != err {
		return errors.Trace(err)
	}
	err = r.st.Set(storageKeyPosition, string(v))
	if nil != err {
		return errors.Trace(err)
	}
	err = r.saveLazy()
	if nil != err {
		return errors.Trace(err)
	}
	return nil
}

func (r *storageReaderWriter) saveLazy() error {
	if time.Now().Unix()-r.lastSaveTime > lazySaveTime {
		err := r.savePositive()
		if nil != err {
			return errors.Trace(err)
		}
	}
	return nil
}

func (r *storageReaderWriter) savePositive() error {
	err := r.st.Save()
	if nil != err {
		return errors.Trace(err)
	}
	r.lastSaveTime = time.Now().Unix()
	return nil
}
