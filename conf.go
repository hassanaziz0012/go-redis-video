package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	config_fp   string
	dir         string
	rdb         []RDBSnapshot
	rdbFn       string
	aofEnabled  bool
	aofFn       string
	aofFsync    FSyncMode
	requirepass bool
	password    string
	maxmem      int64
	eviction    Eviction
	memSamples  int
}

func NewConfig() *Config {
	return &Config{}
}

type RDBSnapshot struct {
	Secs        int
	KeysChanged int
}

type FSyncMode string

const (
	Always   FSyncMode = "always"
	EverySec FSyncMode = "everysec"
	No       FSyncMode = "no"
)

type Eviction string

const (
	NoEviction     Eviction = "noeviction"
	AllKeysRandom  Eviction = "allkeys-random"
	AllKeysLRU     Eviction = "allkeys-lru"
	AllKeysLFU     Eviction = "allkeys-lfu"
	VolatileRandom Eviction = "volatile-random"
	VolatileLRU    Eviction = "volatile-lru"
	VolatileLFU    Eviction = "volatile-lfu"
	VolatileTTL    Eviction = "volatile-ttl"
)

func readConf(fn string) *Config {
	conf := NewConfig()

	f, err := os.Open(fn)
	if err != nil {
		fmt.Printf("cannot read %s - using default config\n", fn)
		return conf
	}
	defer f.Close()

	conf.config_fp = fn

	s := bufio.NewScanner(f)

	for s.Scan() {
		l := s.Text()
		parseLine(l, conf)
	}

	if err := s.Err(); err != nil {
		fmt.Println("error scanning config file: ", err)
		return conf
	}

	if conf.dir != "" {
		os.MkdirAll(conf.dir, 0755)
	}

	return conf
}

func parseLine(l string, conf *Config) {
	args := strings.Split(l, " ")
	cmd := args[0]

	switch cmd {
	case "save":
		secs, err := strconv.Atoi(args[1])
		if err != nil {
			fmt.Println("invalid secs")
			return
		}

		keysChanged, err := strconv.Atoi(args[2])
		if err != nil {
			fmt.Println("invalid keys")
			return
		}

		snapshot := RDBSnapshot{
			Secs:        secs,
			KeysChanged: keysChanged,
		}
		conf.rdb = append(conf.rdb, snapshot)
	case "dbfilename":
		conf.rdbFn = args[1]
	case "appendfilename":
		conf.aofFn = args[1]
	case "appendfsync":
		conf.aofFsync = FSyncMode(args[1])
	case "dir":
		conf.dir = args[1]
	case "appendonly":
		if args[1] == "yes" {
			conf.aofEnabled = true
		} else {
			conf.aofEnabled = false
		}
	case "requirepass":
		conf.requirepass = true
		conf.password = args[1]
	case "maxmemory":
		maxmem, err := parseMem(args[1])
		if err != nil {
			log.Println("cannot parse maxmemory. defaulting to 0. error: ", err)
			conf.maxmem = 0
			break
		}
		conf.maxmem = maxmem
	case "maxmemory-policy":
		conf.eviction = Eviction(args[1])
	case "maxmemory-samples":
		memSamples, err := strconv.Atoi(args[1])
		if err != nil {
			log.Println("cannot parse maxmemory-samples. defaulting to 50. error: ", err)
			conf.memSamples = 50
			break
		}
		conf.memSamples = memSamples
	}
}

func parseMem(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToLower(s))

	var multiplier int64 = 1
	switch {
	case strings.HasSuffix(s, "kb"):
		multiplier = 1024
		s = strings.TrimSuffix(s, "kb")
	case strings.HasSuffix(s, "mb"):
		multiplier = 1024 * 1024
		s = strings.TrimSuffix(s, "mb")
	case strings.HasSuffix(s, "gb"):
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimSuffix(s, "gb")
	case strings.HasSuffix(s, "b"):
		multiplier = 1
		s = strings.TrimSuffix(s, "b")
	}

	num, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}

	return num * multiplier, nil
}
