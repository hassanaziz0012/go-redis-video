package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
)

type Aof struct {
	w    *Writer
	f    *os.File
	conf *Config
}

func NewAof(conf *Config) *Aof {
	aof := Aof{conf: conf}

	fp := path.Join(aof.conf.dir, aof.conf.aofFn)
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644) // owner (read-write), everyone (read)
	if err != nil {
		fmt.Println("cannot open: ", fp)
		return &aof
	}
	aof.w = NewWriter(f)
	aof.f = f

	return &aof
}

func (aof *Aof) Sync() {
	for {
		v := Value{}
		err := v.readArray(aof.f)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("unexpected error while reading AOF records: ", err)
			break
		}

		blankState := NewAppState(&Config{})
		set(&v, blankState)
	}
}
