package main

import (
	"log"
	"maps"
	"path/filepath"
	"strconv"
	"time"
)

type Handler func(*Client, *Value, *AppState) *Value

var Handlers = map[string]Handler{
	"COMMAND":      command,
	"GET":          get,
	"SET":          set,
	"DEL":          del,
	"EXISTS":       exists,
	"KEYS":         keys,
	"SAVE":         save,
	"BGSAVE":       bgsave,
	"FLUSHDB":      flushdb,
	"DBSIZE":       dbsize,
	"AUTH":         auth,
	"EXPIRE":       expire,
	"TTL":          ttl,
	"BGREWRITEAOF": bgrewriteaof,
	"MULTI":        multi,
	"EXEC":         _exec,
	"DISCARD":      discard,
}

var SafeCMDs = []string{
	"COMMAND",
	"AUTH",
}

func handle(c *Client, v *Value, state *AppState) {
	cmd := v.array[0].bulk
	handler, ok := Handlers[cmd]
	w := NewWriter(c.conn)

	if !ok {
		w.Write(&Value{typ: ERROR, err: "ERR invalid command"})
		w.Flush()
		return
	}

	if state.conf.requirepass && !c.authenticated && !contains(SafeCMDs, cmd) {
		w.Write(&Value{typ: ERROR, err: "NOAUTH authentication required"})
		w.Flush()
		return
	}

	if state.tx != nil && cmd != "EXEC" && cmd != "DISCARD" {
		txCmd := TxCommand{v: v, handler: handler}
		state.tx.cmds = append(state.tx.cmds, &txCmd)
		w.Write(&Value{typ: STRING, str: "QUEUED"})
		w.Flush()
		return
	}

	reply := handler(c, v, state)
	w.Write(reply)
	w.Flush()
}

func get(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	if len(args) != 1 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'GET' command"}
	}

	name := args[0].bulk
	item, ok := DB.Get(name)

	if !ok {
		return &Value{typ: NULL}
	}

	return &Value{typ: BULK, bulk: item.V}
}

func set(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	if len(args) != 2 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'SET' command"}
	}

	key := args[0].bulk
	val := args[1].bulk
	DB.mu.Lock()
	err := DB.Set(key, val, state)
	if err != nil {
		DB.mu.Unlock()
		return &Value{typ: ERROR, err: "ERR " + err.Error()}
	}

	if state.conf.aofEnabled {
		log.Println("saving AOF record")
		state.aof.w.Write(v)

		if state.conf.aofFsync == Always {
			state.aof.w.Flush()
		}
	}

	if len(state.conf.rdb) > 0 {
		IncrRDBTrackers()
	}
	DB.mu.Unlock()

	return &Value{typ: STRING, str: "OK"}
}

func del(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	var n int

	DB.mu.Lock()
	for _, arg := range args {
		_, ok := DB.store[arg.bulk]
		DB.Delete(arg.bulk)
		if ok {
			n++
		}
	}
	DB.mu.Unlock()

	return &Value{typ: INTEGER, num: n}
}

func exists(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	var n int

	DB.mu.RLock()
	for _, arg := range args {
		_, ok := DB.store[arg.bulk]
		if ok {
			n++
		}
	}
	DB.mu.RUnlock()

	return &Value{typ: INTEGER, num: n}
}

func keys(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	if len(args) > 1 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'KEYS' command"}
	}
	pattern := args[0].bulk

	DB.mu.RLock()
	var matches []string
	for key := range DB.store {
		matched, err := filepath.Match(pattern, key)
		if err != nil {
			log.Printf("error matching keys: (pattern: %s), (key: %s) - %v", pattern, key, err)
			continue
		}

		if matched {
			matches = append(matches, key)
		}
	}
	DB.mu.RUnlock()

	reply := Value{typ: ARRAY}

	for _, m := range matches {
		reply.array = append(reply.array, Value{typ: BULK, bulk: m})
	}
	return &reply
}

func save(c *Client, v *Value, state *AppState) *Value {
	SaveRDB(state)
	return &Value{typ: STRING, str: "OK"}
}

func bgsave(c *Client, v *Value, state *AppState) *Value {
	if state.bgsaveRunning {
		return &Value{typ: ERROR, err: "ERR background saving already in progress"}
	}

	cp := make(map[string]*Item, len(DB.store))
	DB.mu.RLock()
	maps.Copy(cp, DB.store)
	DB.mu.RUnlock()

	state.bgsaveRunning = true
	state.dbCopy = cp

	go func() {
		defer func() {
			state.bgsaveRunning = false
			state.dbCopy = nil
		}()

		SaveRDB(state)
	}()

	return &Value{typ: STRING, str: "OK"}
}

func flushdb(c *Client, v *Value, state *AppState) *Value {
	DB.mu.Lock()
	DB.store = map[string]*Item{}
	DB.mu.Unlock()

	return &Value{typ: STRING, str: "OK"}
}

func dbsize(c *Client, v *Value, state *AppState) *Value {
	DB.mu.RLock()
	size := len(DB.store)
	DB.mu.RUnlock()

	return &Value{typ: INTEGER, num: size}
}

func auth(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	if len(args) != 1 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'AUTH' command"}
	}

	p := args[0].bulk
	if state.conf.password == p {
		c.authenticated = true
		return &Value{typ: STRING, str: "OK"}
	} else {
		c.authenticated = false
		return &Value{typ: ERROR, err: "ERR invalid password"}
	}
}

func expire(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	if len(args) != 2 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'EXPIRE' command"}
	}

	k := args[0].bulk
	exp := args[1].bulk

	expSecs, err := strconv.Atoi(exp)
	if err != nil {
		return &Value{typ: ERROR, err: "ERR invalid expiry value"}
	}

	DB.mu.RLock()
	key, ok := DB.store[k]
	if !ok {
		return &Value{typ: INTEGER, num: 0}
	}
	key.Exp = time.Now().Add(time.Second * time.Duration(expSecs))
	DB.mu.RUnlock()

	return &Value{typ: INTEGER, num: 1}
}

func ttl(c *Client, v *Value, state *AppState) *Value {
	args := v.array[1:]
	if len(args) != 1 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'TTL' command"}
	}

	k := args[0].bulk

	DB.mu.RLock()
	key, ok := DB.store[k]
	if !ok {
		return &Value{typ: INTEGER, num: -2}
	}
	exp := key.Exp
	DB.mu.RUnlock()

	if exp.Unix() == UNIX_TS_EPOCH {
		return &Value{typ: INTEGER, num: -1}
	}

	expSecs := int(time.Until(exp).Seconds())
	if expSecs <= 0 {
		DB.mu.Lock()
		DB.Delete(k)
		DB.mu.Unlock()
		return &Value{typ: INTEGER, num: -2}
	}

	return &Value{typ: INTEGER, num: expSecs}
}

func bgrewriteaof(c *Client, v *Value, state *AppState) *Value {
	go func() {
		DB.mu.RLock()
		cp := make(map[string]*Item, len(DB.store))
		maps.Copy(cp, DB.store)
		DB.mu.RUnlock()

		state.aof.Rewrite(cp)
	}()

	return &Value{typ: STRING, str: "Background AOF rewriting started"}
}

func multi(c *Client, v *Value, state *AppState) *Value {
	if state.tx != nil {
		return &Value{typ: ERROR, err: "ERR MULTI calls can not be nested"}
	}

	state.tx = NewTransaction()

	return &Value{typ: STRING, str: "OK"}
}

func _exec(c *Client, v *Value, state *AppState) *Value {
	if state.tx == nil {
		return &Value{typ: ERROR, err: "ERR EXEC without MULTI"}
	}

	replies := make([]Value, len(state.tx.cmds))
	for i, cmd := range state.tx.cmds {
		reply := cmd.handler(c, cmd.v, state)
		replies[i] = *reply
	}

	reply := Value{typ: ARRAY, array: replies}

	state.tx = nil

	return &reply
}

func discard(c *Client, v *Value, state *AppState) *Value {
	if state.tx == nil {
		return &Value{typ: ERROR, err: "ERR DISCARD without MULTI"}
	}

	state.tx = nil
	return &Value{typ: STRING, str: "OK"}
}

func command(c *Client, v *Value, state *AppState) *Value {
	return &Value{typ: STRING, str: "OK"}
}
