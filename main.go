package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

var UNIX_TS_EPOCH int64 = -62135596800

func main() {
	log.Println("reading config file")
	conf := readConf("./redis.conf")

	state := NewAppState(conf)

	if conf.aofEnabled {
		log.Println("syncing AOF records")
		state.aof.Sync(conf.maxmem, conf.eviction, conf.memSamples)
	}

	if len(conf.rdb) > 0 {
		SyncRDB(conf)
		InitRDBTrackers(state)
	}

	l, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal("cannot listen on :6379")
	}
	defer l.Close()
	log.Println("listening on :6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		log.Println("connection accepted")

		go func() {
			handleConn(conn, state)
		}()
	}
}

func handleConn(conn net.Conn, state *AppState) {
	log.Println("accepted new connection: ", conn.LocalAddr().String())
	c := NewClient(conn)
	r := bufio.NewReader(conn)

	defer func() {
		new := state.monitors[:0]
		for _, mon := range state.monitors {
			if mon != c {
				new = append(new, mon)
			}
		}
		state.monitors = new
	}()

	state.clientCount++
	defer func() {
		state.clientCount--
	}()
	state.generalStats.total_connections_received++

	for {
		v := Value{typ: ARRAY}
		if err := v.readArray(r); err != nil {
			log.Println(err)
			break
		}
		handle(c, &v, state)
	}
	log.Println("connection closed: ", conn.LocalAddr().String())
}
