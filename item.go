package main

import "time"

type Item struct {
	V          string
	Exp        time.Time
	LastAccess time.Time
	Accesses   int
}

func (item *Item) shouldExpire() bool {
	return item.Exp.Unix() != UNIX_TS_EPOCH && time.Until(item.Exp).Seconds() <= 0
}

func (item *Item) approxMemUsage(name string) int64 {
	stringHeader := 16
	expHeader := 24
	mapEntrySize := 32

	return int64(stringHeader + len(name) + stringHeader + len(item.V) + expHeader + mapEntrySize)
}
