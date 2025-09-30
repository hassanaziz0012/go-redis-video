package main

import (
	"bufio"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"
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

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

func (v *Value) readArray(r *bufio.Reader) error {
	line, err := readLine(r)
	if err != nil {
		return err
	}

	if line[0] != '*' {
		return errors.New("expected array")
	}

	arrLen, err := strconv.Atoi(line[1:])
	if err != nil {
		return err
	}

	for range arrLen {
		bulk, err := v.readBulk(r)
		if err != nil {
			log.Println(err)
			break
		}
		v.array = append(v.array, bulk)
	}

	return nil
}

func (v *Value) readBulk(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}

	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return Value{}, err
	}

	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Value{}, err
	}

	bulk := string(buf[:n])
	return Value{typ: BULK, bulk: bulk}, nil
}
