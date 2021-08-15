package clickhouse

import (
	"time"
)

const AddAction = "add"
const TimeTickerAction = "time_ticker"
const ExecuteAction = "execute"

type RowsExecuteMessage struct {
	Action    string
	Key       string
	Row       []interface{}
	Timestamp time.Time
}
