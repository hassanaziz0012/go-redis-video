package main

import (
	"fmt"
	"io"
	"strconv"
)

type ValueType string

const (
	ARRAY   ValueType = "*"
	BULK    ValueType = "$"
	STRING  ValueType = "+"
	INTEGER ValueType = ":"
	ERROR   ValueType = "-"
	NULL    ValueType = ""
)

type Value struct {
	typ   ValueType
	bulk  string
	str   string
	num   int
	err   string
	array []Value
}

func (v *Value) readArray(reader io.Reader) error {
	buf := make([]byte, 4)
	_, err := reader.Read(buf)
	if err != nil {
		return err
	}

	arrLen, err := strconv.Atoi(string(buf[1]))
	if err != nil {
		return err
	}

	for range arrLen {
		bulk := v.readBulk(reader)
		v.array = append(v.array, bulk)
	}

	return nil
}

func (v *Value) readBulk(reader io.Reader) Value {
	buf := make([]byte, 4)
	reader.Read(buf)

	n, err := strconv.Atoi(string(buf[1]))
	if err != nil {
		fmt.Println(err)
		return Value{}
	}

	bulkBuf := make([]byte, n+2)
	reader.Read(bulkBuf)

	bulk := string(bulkBuf[:n])
	return Value{typ: BULK, bulk: bulk}
}
