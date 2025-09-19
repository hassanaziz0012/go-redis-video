package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
)

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: bufio.NewWriter(w)}
}

func (w *Writer) Deserialize(v *Value) (reply string) {
	switch v.typ {
	case ARRAY:
		reply = fmt.Sprintf("*%d\r\n", len(v.array))
		for _, sub := range v.array {
			reply += w.Deserialize(&sub)
		}
	case STRING:
		reply = fmt.Sprintf("%s%s\r\n", v.typ, v.str)
	case INTEGER:
		reply = fmt.Sprintf("%s%d\r\n", v.typ, v.num)
	case BULK:
		reply = fmt.Sprintf("%s%d\r\n%s\r\n", v.typ, len(v.bulk), v.bulk)
	case ERROR:
		reply = fmt.Sprintf("%s%s\r\n", v.typ, v.err)
	case NULL:
		reply = "$-1\r\n"
	default:
		log.Println("invalid typ received")
		return reply
	}

	return reply
}

func (w *Writer) Write(v *Value) {
	reply := w.Deserialize(v)
	w.writer.Write([]byte(reply))
}

func (w *Writer) Flush() {
	w.writer.(*bufio.Writer).Flush()
}
