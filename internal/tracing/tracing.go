package tracing

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// TraceEvent 代表一個 tracing 事件
type TraceEvent struct {
	Timestamp     time.Time     `json:"timestamp"`
	Activity      string        `json:"activity"`
	Source        string        `json:"source"`         // 處理該事件的節點 IP
	SourceElapsed time.Duration `json:"source_elapsed"` // 該事件的耗時
	Thread        string        `json:"thread"`
}

// TraceResult 彙整一次查詢的 tracing 結果
type TraceResult struct {
	TraceID      string        `json:"trace_id"`
	Coordinator  string        `json:"coordinator"`   // 協調節點 IP
	Duration     time.Duration `json:"duration"`      // 整體耗時
	ReplicaNodes []string      `json:"replica_nodes"` // 實際參與的 replica 節點 IP
	Events       []TraceEvent  `json:"events"`        // 完整事件列表
	Summary      string        `json:"summary"`       // 易讀摘要
}

// QueryTracer 實作 gocql.Tracer 介面，收集查詢的 tracing 資訊
type QueryTracer struct {
	session *gocql.Session
	mu      sync.Mutex
	result  *TraceResult
}

// NewQueryTracer 建立新的 QueryTracer
func NewQueryTracer(session *gocql.Session) *QueryTracer {
	return &QueryTracer{
		session: session,
	}
}

// Trace 實作 gocql.Tracer 介面
// 當查詢完成後，gocql 會自動呼叫此方法並傳入 traceId
func (t *QueryTracer) Trace(traceId []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()

	traceUUID, _ := gocql.UUIDFromBytes(traceId)

	result := &TraceResult{
		TraceID: traceUUID.String(),
	}

	// 查詢 system_traces.sessions 取得 coordinator 和 duration
	var coordinator string
	var durationMicros int

	// ScyllaDB tracing 是非同步寫入的，需要等待一小段時間
	time.Sleep(100 * time.Millisecond)

	iter := t.session.Query(
		`SELECT coordinator, duration FROM system_traces.sessions WHERE session_id = ?`,
		traceUUID,
	).Consistency(gocql.One).Iter()
	iter.Scan(&coordinator, &durationMicros)
	iter.Close()

	result.Coordinator = coordinator
	result.Duration = time.Duration(durationMicros) * time.Microsecond

	// 查詢 system_traces.events 取得事件列表
	nodeSet := make(map[string]struct{})
	if coordinator != "" {
		nodeSet[coordinator] = struct{}{}
	}

	var (
		timestamp time.Time
		activity  string
		source    string
		elapsed   int
		thread    string
	)

	eventIter := t.session.Query(
		`SELECT event_id, activity, source, source_elapsed, thread FROM system_traces.events WHERE session_id = ?`,
		traceUUID,
	).Consistency(gocql.One).Iter()

	for eventIter.Scan(&timestamp, &activity, &source, &elapsed, &thread) {
		event := TraceEvent{
			Timestamp:     timestamp,
			Activity:      activity,
			Source:        source,
			SourceElapsed: time.Duration(elapsed) * time.Microsecond,
			Thread:        thread,
		}
		result.Events = append(result.Events, event)

		// 收集所有參與的 node IP
		if source != "" {
			nodeSet[source] = struct{}{}
		}
	}
	eventIter.Close()

	// 收集所有 replica nodes（去重）
	for node := range nodeSet {
		result.ReplicaNodes = append(result.ReplicaNodes, node)
	}

	// 產生易讀摘要
	result.Summary = fmt.Sprintf(
		"Coordinator: %s | Replicas: [%s] | Duration: %v | Events: %d",
		result.Coordinator,
		strings.Join(result.ReplicaNodes, ", "),
		result.Duration,
		len(result.Events),
	)

	t.result = result
}

// GetResult 取得 tracing 結果
func (t *QueryTracer) GetResult() *TraceResult {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.result
}

// FormatTraceLog 產生格式化的 log 輸出字串
func FormatTraceLog(tr *TraceResult) string {
	if tr == nil {
		return "[Tracing] No trace data available"
	}

	var sb strings.Builder
	sb.WriteString("\n╔══════════════════════════════════════════════════════════\n")
	sb.WriteString("║ 🔍 SCYLLADB TRACING RESULT\n")
	sb.WriteString("╠══════════════════════════════════════════════════════════\n")
	sb.WriteString(fmt.Sprintf("║ Trace ID    : %s\n", tr.TraceID))
	sb.WriteString(fmt.Sprintf("║ Coordinator : %s\n", tr.Coordinator))
	sb.WriteString(fmt.Sprintf("║ Duration    : %v\n", tr.Duration))
	sb.WriteString(fmt.Sprintf("║ Replicas    : [%s]\n", strings.Join(tr.ReplicaNodes, ", ")))
	sb.WriteString("╠══════════════════════════════════════════════════════════\n")
	sb.WriteString("║ EVENTS:\n")

	for i, e := range tr.Events {
		sb.WriteString(fmt.Sprintf("║  [%02d] %-50s | node=%-15s | elapsed=%v\n",
			i+1, e.Activity, e.Source, e.SourceElapsed))
	}

	sb.WriteString("╚══════════════════════════════════════════════════════════\n")
	return sb.String()
}
