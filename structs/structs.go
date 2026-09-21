package structs

import (
	"time"
)

type KVValue struct {
	Value    string
	TimeOut  time.Duration
	InitTime time.Time
}

type Topic string
type SubscriptionID string

// BUG FIX: this used to be `map[Topic]map[SubscriptionID]chan string`,
// i.e. a live reference into the server's internal broker map. That's
// unsafe for two independent reasons:
//  1. Go channels aren't JSON-marshalable, so json.Marshal on this field
//     would silently fail (the error was being discarded with `_`).
//  2. It exposed the live map with no lock protection to whoever held
//     this struct, which is what caused the "concurrent map iteration
//     and map write" panic in the admin dashboard.
//
// It's now a plain, already-safe snapshot: topic name -> subscriber count.
type SrvData struct {
	KVMapLength    int64                       `json:"kv_map_length"`
	Topics         map[string]map[string]int64 `json:"topics"`
	SystemRamUsage float64                     `json:"system_ram_usage"`
	HeapAllocMB    uint64                      `json:"heap_alloc_mb"`
	TotalAllocMB   uint64                      `json:"total_alloc_mb"`
	SysMB          uint64                      `json:"sys_mb"`
	NumGC          uint32                      `json:"num_gc"`
	CPUUsage       float64                     `json:"cpu_usage"`
}

type AdminResponse struct {
	QueueMetric SrvData `json:"queue_metric"`
}
