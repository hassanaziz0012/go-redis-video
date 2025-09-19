package main

import (
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"io"
	"log"
	"os"
	"path"
	"time"
)

type SnapshotTracker struct {
	keys   int
	ticker time.Ticker
	rdb    *RDBSnapshot
}

func NewSnapshotTracker(rdb *RDBSnapshot) *SnapshotTracker {
	return &SnapshotTracker{
		keys:   0,
		ticker: *time.NewTicker(time.Second * time.Duration(rdb.Secs)),
		rdb:    rdb,
	}
}

var trackers = []*SnapshotTracker{}

func InitRDBTrackers(state *AppState) {
	for _, rdb := range state.conf.rdb {
		tracker := NewSnapshotTracker(&rdb)
		trackers = append(trackers, tracker)

		go func() {
			defer tracker.ticker.Stop()

			for range tracker.ticker.C {
				log.Printf("keys changed: %d - keys required to change: %d", tracker.keys, tracker.rdb.KeysChanged)
				if tracker.keys >= tracker.rdb.KeysChanged {
					SaveRDB(state)
				}
				tracker.keys = 0
			}
		}()
	}
}

func IncrRDBTrackers() {
	for _, t := range trackers {
		t.keys++
	}
}

func SaveRDB(state *AppState) {
	fp := path.Join(state.conf.dir, state.conf.rdbFn)
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644) // owner (read-write), everyone else (read)
	if err != nil {
		log.Println("error opening rdb file: ", err)
		return
	}
	defer f.Close()

	log.Println("saving DB to RDB file")
	if state.bgsaveRunning {
		err = gob.NewEncoder(f).Encode(&state.dbCopy)
	} else {
		DB.mu.RLock()
		err = gob.NewEncoder(f).Encode(&DB.store)
		DB.mu.RUnlock()
	}

	if err != nil {
		log.Println("error saving rdb file: ", err)
		return
	}

	log.Println("saved RDB file")
}

func SyncRDB(conf *Config) {
	fp := path.Join(conf.dir, conf.rdbFn)
	f, err := os.Open(fp)
	if err != nil {
		log.Println("error opening rdb file: ", err)
		f.Close()
		return
	}
	defer f.Close()

	err = gob.NewDecoder(f).Decode(&DB.store)
	if err != nil {
		log.Println("error decoding rdb file: ", err)
		return
	}
}

func Hash(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
