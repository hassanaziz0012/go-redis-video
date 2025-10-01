package main

import (
	"fmt"
	"log"
	"net"
	"time"
)

type Client struct {
	conn          net.Conn
	authenticated bool
}

func NewClient(conn net.Conn) *Client {
	return &Client{conn: conn}
}

func (c *Client) writeMonitorLog(v *Value) {
	log.Println("relaying command to monitor: ", c.conn.LocalAddr().String())

	msg := fmt.Sprintf("%d [%s]", time.Now().Unix(), c.conn.LocalAddr().String())
	for _, v := range v.array {
		msg += fmt.Sprintf("\"%s\"", v.bulk)
	}

	reply := Value{typ: STRING, str: msg}
	w := NewWriter(c.conn)
	w.Write(&reply)
	w.Flush()
}
