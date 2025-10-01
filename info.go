package main

import (
	"fmt"
	"os"
	"time"

	"github.com/shirou/gopsutil/v4/mem"
)

type Info struct {
	server      map[string]string
	client      map[string]string
	memory      map[string]string
	persistence map[string]string
	general     map[string]string
}

func NewInfo() *Info {
	return &Info{}
}

func (info *Info) build(state *AppState) {
	excPath, err := os.Executable()
	if err != nil {
		excPath = ""
	}

	memory, err := mem.VirtualMemory()
	var memTotal uint64
	if err != nil {
		memTotal = 0
	} else {
		memTotal = memory.Total
	}

	info.server = map[string]string{
		"redis_version":     "1.0.0",
		"process_id":        fmt.Sprint(os.Getpid()),
		"tcp_port":          "6379",
		"server_time_usec":  fmt.Sprint(time.Now().UnixMicro()),
		"uptime_in_seconds": fmt.Sprint(int(time.Since(state.serverStart).Seconds())),
		"executable":        excPath,
		"config_file":       state.conf.config_fp,
	}

	info.client = map[string]string{
		"connected_clients": fmt.Sprint(state.clientCount),
	}

	info.memory = map[string]string{
		"used_memory":         fmt.Sprint(DB.mem),
		"used_memory_peak":    fmt.Sprint(state.peakMem),
		"total_system_memory": fmt.Sprint(memTotal),
		"maxmemory":           fmt.Sprint(state.conf.maxmem),
		"maxmemory_policy":    string(state.conf.eviction),
	}

	info.persistence = map[string]string{
		"rdb_bgsave_in_process":   fmt.Sprint(state.bgsaveRunning),
		"rdb_last_save_time":      fmt.Sprint(state.rdbStats.rdb_last_save_ts),
		"rdb_saves":               fmt.Sprint(state.rdbStats.rdb_saves),
		"aof_enabled":             fmt.Sprint(state.conf.aofEnabled),
		"aof_rewrite_in_progress": fmt.Sprint(state.aofRewriteRunning),
		"aof_rewrites":            fmt.Sprint(state.aofStats.aof_rewrites),
	}

	info.general = map[string]string{
		"total_connections_received": fmt.Sprint(state.generalStats.total_connections_received),
		"total_commands_processed":   fmt.Sprint(state.generalStats.total_commands_processed),
		"evicted_keys":               fmt.Sprint(state.generalStats.evicted_keys),
		"expired_keys":               fmt.Sprint(state.generalStats.expired_keys),
	}
}

func (info *Info) print(state *AppState) string {
	info.build(state)

	var msg string = ""

	printCategory := func(header string, m map[string]string) string {
		s := fmt.Sprintf("# %s\n", header)
		for k, v := range m {
			s += fmt.Sprintf("%s:%s\n", k, v)
		}
		return s + "\n"
	}

	msg += printCategory("Server", info.server)
	msg += printCategory("Client", info.client)
	msg += printCategory("Memory", info.memory)
	msg += printCategory("Persistence", info.persistence)
	msg += printCategory("General", info.general)

	return msg
}
