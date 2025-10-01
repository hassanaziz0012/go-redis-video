package main

import (
	"bufio"
	"bytes"
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

func (aof *Aof) Sync(maxmem int64, evictionpolicy Eviction, memsamples int) {
	r := bufio.NewReader(aof.f)
	for {
		v := Value{}
		err := v.readArray(r)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("unexpected error while reading AOF records: ", err)
			break
		}

		blankState := NewAppState(&Config{
			maxmem:     maxmem,
			eviction:   evictionpolicy,
			memSamples: memsamples,
		})
		blankClient := Client{}
		set(&blankClient, &v, blankState)
	}
}

func (aof *Aof) Rewrite(cp map[string]*Item) {
	// reroute future AOF records to buffer
	var b bytes.Buffer
	aof.w = NewWriter(&b)

	// clear file contents
	if err := aof.f.Truncate(0); err != nil {
		log.Println("aof rewrite - truncate error: ", err)
		return
	}
	if _, err := aof.f.Seek(0, 0); err != nil {
		log.Println("aof rewrite - seek error: ", err)
		return
	}

	// write all SET commands to file
	fwriter := NewWriter(aof.f)
	for k, v := range cp {
		cmd := Value{typ: BULK, bulk: "SET"}
		key := Value{typ: BULK, bulk: k}
		val := Value{typ: BULK, bulk: v.V}

		arr := Value{typ: ARRAY, array: []Value{
			cmd, key, val,
		}}
		fwriter.Write(&arr)
	}
	fwriter.Flush()

	// reroute future AOF records back to file
	aof.w = NewWriter(aof.f)
}
