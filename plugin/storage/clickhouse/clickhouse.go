// Copyright (c) 2019 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/clickhouse/config"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

// Store is an in-clickhouse store of traces
type Store struct {
	logger             *zap.Logger
	conn               *sql.DB
	lastCommitMap      map[string]time.Time
	rowsMap            map[string][][]interface{}
	rowsChan           chan RowsExecuteMessage
	config             config.Configuration
	YYMMDDHHIISSFormat string
	YYMMDDFormat       string
	TimeZone           *time.Location
}

// WithConfiguration creates a new in clickhouse storage based on the given configuration
func WithConfiguration(configuration config.Configuration, logger *zap.Logger) *Store {
	conn, err := sql.Open("clickhouse", configuration.DataSourceName)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	if err := conn.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Println(fmt.Sprintf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace))
		} else {
			fmt.Println(err)
		}
	}
	TimeZone, _ := time.LoadLocation("Asia/Shanghai")

	return &Store{
		logger:             logger,
		rowsChan:           make(chan RowsExecuteMessage, 1000),
		rowsMap:            map[string][][]interface{}{},
		lastCommitMap:      map[string]time.Time{},
		conn:               conn,
		config:             configuration,
		YYMMDDHHIISSFormat: "2006-01-02 15:04:05",
		YYMMDDFormat:       "2006-01-02",
		TimeZone:           TimeZone,
	}
}

func FormatInsertSQL(table string, columns []string) string {
	colStr := strings.Join(columns, ",")
	placeholder := strings.Trim(strings.Repeat("?,", len(columns)), ",")
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, colStr, placeholder)
}

func (m *Store) MultiInsertClickHouse(table string, rows [][]interface{}) {
	tx, err := m.conn.Begin()
	if err != nil {
		m.logger.Error(err.Error())
		return
	}
	stmt, err := tx.Prepare(FormatInsertSQL(table, []string{"date", "service_name", "operation_name", "trace_id", "span_id", "reference_trace_id", "reference_span_id", "reference_ref_type", "start_time", "duration", "process", "tags", "logs"}))
	if err != nil {
		m.logger.Error(err.Error())
		return
	}
	for _, row := range rows {
		if row == nil || len(row) <= 0 {
			continue
		}
		_, err := stmt.Exec(row...)
		if err != nil {
			m.logger.Error(err.Error())
			return
		}
	}
	err = tx.Commit()
	if err != nil {
		m.logger.Error(err.Error())
		return
	}
}

// GetDependencies returns dependencies between services
func (m *Store) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	retMe := make([]model.DependencyLink, 0)
	return retMe, nil
}

func (m *Store) findSpan(trace *model.Trace, spanID model.SpanID) *model.Span {
	for _, s := range trace.Spans {
		if s.SpanID == spanID {
			return s
		}
	}
	return nil
}

func (m *Store) traceIsBetweenStartAndEnd(startTs, endTs time.Time, trace *model.Trace) bool {
	for _, s := range trace.Spans {
		if s.StartTime.After(startTs) && endTs.After(s.StartTime) {
			return true
		}
	}
	return false
}

type TagMap struct {
	Type  int32
	Value string
}

func formatKeyValues2Map(tags []model.KeyValue) map[string]TagMap {
	var tagsMap = map[string]TagMap{}
	for _, tag := range tags {
		v := ""
		switch tag.VType {
		case model.StringType:
			v = tag.GetVStr()
		case model.Int64Type:
			v = strconv.FormatInt(tag.GetVInt64(), 10)
		case model.BoolType:
			v = strconv.FormatBool(tag.GetVBool())
		case model.Float64Type:
			v = strconv.FormatFloat(tag.GetVFloat64(), 'E', -1, 64)
		case model.BinaryType:
			v = string(tag.GetVBinary())
		}
		tagsMap[tag.Key] = TagMap{
			Type:  int32(tag.VType),
			Value: v,
		}
	}
	return tagsMap
}

func formatMap2KeyValues(maps map[string]TagMap) []model.KeyValue {
	var tags []model.KeyValue
	for key, tagMap := range maps {
		tag := model.KeyValue{
			Key:   key,
			VType: model.ValueType(tagMap.Type),
		}
		switch tag.VType {
		case model.StringType:
			tag.VStr = tagMap.Value
		case model.Int64Type:
			tag.VInt64, _ = strconv.ParseInt(tagMap.Value, 10, 64)
		case model.BoolType:
			tag.VBool = tagMap.Value == "true"
		case model.Float64Type:
			tag.VFloat64, _ = strconv.ParseFloat(tagMap.Value, 64)
		case model.BinaryType:
			tag.VBinary = []byte(tagMap.Value)
		}
		tags = append(tags, tag)
	}
	return tags
}

// WriteSpan writes the given span
func (m *Store) WriteSpan(ctx context.Context, span *model.Span) error {

	processTagsBytes, _ := json.Marshal(formatKeyValues2Map(span.Process.Tags))
	tagsBytes, _ := json.Marshal(formatKeyValues2Map(span.Tags))

	logsBytes, _ := json.Marshal(span.Logs)

	referenceTraceId := ""
	referenceSpanId := ""
	referenceRefType := ""

	if len(span.References) >= 1 {
		referenceTraceId = span.References[0].TraceID.String()
		referenceSpanId = span.References[0].SpanID.String()
		referenceRefType = span.References[0].RefType.String()
	}

	m.rowsChan <- RowsExecuteMessage{
		Action: AddAction,
		Key:    "jaeger_spans",
		Row: []interface{}{
			span.StartTime.In(m.TimeZone),
			span.Process.ServiceName,
			span.OperationName,
			span.TraceID.String(),
			span.SpanID.String(),
			referenceTraceId,
			referenceSpanId,
			referenceRefType,
			span.StartTime.UnixNano(),
			span.Duration.Nanoseconds(),
			string(processTagsBytes),
			string(tagsBytes),
			string(logsBytes),
		},
	}

	return nil
}

// GetTrace gets a trace
func (m *Store) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {

	now := time.Now()
	SQL := fmt.Sprintf(
		"select service_name,operation_name,trace_id,span_id,reference_trace_id,reference_span_id,reference_ref_type,start_time,duration,process,tags,logs from jaeger_spans_all where date between '%s' and '%s' and trace_id = '%s' order by start_time",
		now.Add(-7*60*60*24*time.Second).In(m.TimeZone).Format(m.YYMMDDFormat),
		now.In(m.TimeZone).Format(m.YYMMDDFormat),
		traceID.String(),
	)
	rows, err := m.conn.Query(
		SQL,
	)
	if err != nil {
		m.logger.Error(err.Error())
		return nil, spanstore.ErrTraceNotFound
	}

	defer rows.Close()

	trace := new(model.Trace)
	for rows.Next() {
		var (
			serviceName      string
			OperationName    string
			traceId          string
			spanId           string
			referenceTraceId string
			referenceSpanId  string
			referenceRefType string
			startTime        int64
			duration         int64
			process          string
			tags             string
			logs             string
		)

		if err := rows.Scan(&serviceName, &OperationName, &traceId, &spanId, &referenceTraceId, &referenceSpanId, &referenceRefType, &startTime, &duration, &process, &tags, &logs); err != nil {
			m.logger.Error(err.Error())
			continue
		}

		var processTagsMap map[string]TagMap
		_ = json.Unmarshal([]byte(process), &processTagsMap)

		var tagsMap map[string]TagMap
		_ = json.Unmarshal([]byte(tags), &tagsMap)

		traceID, _ := model.TraceIDFromString(traceId)
		SpanID, _ := model.SpanIDFromString(spanId)

		var refs []model.SpanRef
		if referenceSpanId != "" {
			ParentTraceID, _ := model.TraceIDFromString(referenceTraceId)
			ParentSpanID, _ := model.SpanIDFromString(referenceSpanId)
			refs = append(refs, model.SpanRef{
				TraceID: ParentTraceID,
				SpanID:  ParentSpanID,
				RefType: model.SpanRefType(model.SpanRefType_value[referenceRefType]),
			})
		} else {
			refs = nil
		}

		span := &model.Span{
			TraceID:       traceID,
			SpanID:        SpanID,
			OperationName: OperationName,
			References:    refs,
			Flags:         model.Flags(uint32(0)),
			StartTime:     time.Unix(startTime/1e9, startTime%1e9),
			Duration:      model.MicrosecondsAsDuration(uint64(duration / 1e3)),
			Tags:          formatMap2KeyValues(tagsMap),
			Logs:          nil,
			Process: &model.Process{
				ServiceName: serviceName,
				Tags:        formatMap2KeyValues(processTagsMap),
			},
		}

		trace.Spans = append(trace.Spans, span)
	}

	if err := rows.Err(); err != nil {
		m.logger.Error(err.Error())
		return nil, spanstore.ErrTraceNotFound
	}

	if len(trace.Spans) <= 0 {
		return nil, spanstore.ErrTraceNotFound
	}

	return m.copyTrace(trace)
}

// Spans may still be added to traces after they are returned to user code, so make copies.
func (m *Store) copyTrace(trace *model.Trace) (*model.Trace, error) {
	bytes, err := proto.Marshal(trace)
	if err != nil {
		return nil, err
	}

	copied := &model.Trace{}
	err = proto.Unmarshal(bytes, copied)
	return copied, err
}

// GetServices returns a list of all known services
func (m *Store) GetServices(ctx context.Context) ([]string, error) {
	var retMe []string

	now := time.Now()
	SQL := fmt.Sprintf(
		"select distinct service_name from jaeger_spans_all where date between '%s' and '%s'",
		now.Add(-7*60*60*24*time.Second).In(m.TimeZone).Format(m.YYMMDDFormat),
		now.In(m.TimeZone).Format(m.YYMMDDFormat),
	)
	rows, err := m.conn.Query(
		SQL,
	)
	if err != nil {
		m.logger.Error(err.Error())
		return nil, nil
	}

	defer rows.Close()

	for rows.Next() {
		var (
			serviceName string
		)
		if err := rows.Scan(&serviceName); err != nil {
			m.logger.Error(err.Error())
			continue
		}
		retMe = append(retMe, serviceName)
	}

	if err := rows.Err(); err != nil {
		m.logger.Error(err.Error())
		return retMe, nil
	}

	return retMe, nil
}

// GetOperations returns the operations of a given service
func (m *Store) GetOperations(
	ctx context.Context,
	query spanstore.OperationQueryParameters,
) ([]spanstore.Operation, error) {
	var retMe []spanstore.Operation
	now := time.Now()
	SQL := fmt.Sprintf(
		"select distinct operation_name from jaeger_spans_all where date between '%s' and '%s' and service_name = '%s'",
		now.Add(-7*60*60*24*time.Second).In(m.TimeZone).Format(m.YYMMDDFormat),
		now.In(m.TimeZone).Format(m.YYMMDDFormat),
		query.ServiceName,
	)
	rows, err := m.conn.Query(
		SQL,
	)
	if err != nil {
		m.logger.Error(err.Error())
		return nil, nil
	}

	defer rows.Close()

	for rows.Next() {
		var (
			operationName string
		)
		if err := rows.Scan(&operationName); err != nil {
			m.logger.Error(err.Error())
			continue
		}
		retMe = append(retMe, spanstore.Operation{
			Name:     operationName,
			SpanKind: "",
		})
	}

	if err := rows.Err(); err != nil {
		m.logger.Error(err.Error())
		return retMe, nil
	}

	return retMe, nil
}

// FindTraces returns all traces in the query parameters are satisfied by a trace's span
func (m *Store) FindTraces(ctx context.Context, query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	var retMe []*model.Trace

	operationNameWhere := ""
	tagsWhere := ""
	durationWhere := ""

	fmt.Println(query.Tags)

	for k, v := range query.Tags {
		tagsWhere += fmt.Sprintf(
			"and (visitParamExtractString(visitParamExtractRaw(process, '%s'), 'Value') = '%s' or visitParamExtractString(visitParamExtractRaw(tags, '%s'), 'Value') = '%s') ",
			k, v, k, v,
		)
	}

	if query.DurationMax.Nanoseconds() != 0 {
		durationWhere = fmt.Sprintf(
			"and duration between %d and %d",
			query.DurationMin.Nanoseconds(),
			query.DurationMax.Nanoseconds(),
		)
	}

	if query.OperationName != "" {
		operationNameWhere = fmt.Sprintf(
			"and operation_name = '%s'",
			query.OperationName,
		)
	}

	SQL := fmt.Sprintf(
		"select distinct trace_id from jaeger_spans_all where date between '%s' and '%s' and service_name = '%s' %s and start_time between %d and %d %s %s order by start_time limit %d",
		query.StartTimeMin.In(m.TimeZone).Format(m.YYMMDDFormat),
		query.StartTimeMax.In(m.TimeZone).Format(m.YYMMDDFormat),
		query.ServiceName,
		operationNameWhere,
		query.StartTimeMin.UnixNano(),
		query.StartTimeMax.UnixNano(),
		durationWhere,
		tagsWhere,
		query.NumTraces,
	)
	rows, err := m.conn.Query(
		SQL,
	)

	fmt.Println(SQL)

	if err != nil {
		m.logger.Error(err.Error())
		return nil, spanstore.ErrTraceNotFound
	}

	defer rows.Close()

	var traceIds []string
	for rows.Next() {
		var traceId string
		if err := rows.Scan(&traceId); err != nil {
			m.logger.Error(err.Error())
			continue
		}

		traceIds = append(traceIds, traceId)
		traceID, _ := model.TraceIDFromString(traceId)
		if trace, err := m.GetTrace(ctx, traceID); err == nil {
			retMe = append(retMe, trace)
		}
	}

	if err := rows.Err(); err != nil {
		m.logger.Error(err.Error())
		return nil, spanstore.ErrTraceNotFound
	}

	return retMe, nil
}

// FindTraceIDs is not implemented.
func (m *Store) FindTraceIDs(ctx context.Context, query *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	return nil, errors.New("not implemented")
}

func (m *Store) validTrace(trace *model.Trace, query *spanstore.TraceQueryParameters) bool {
	for _, span := range trace.Spans {
		if m.validSpan(span, query) {
			return true
		}
	}
	return false
}

func findKeyValueMatch(kvs model.KeyValues, key, value string) (model.KeyValue, bool) {
	for _, kv := range kvs {
		if kv.Key == key && kv.AsString() == value {
			return kv, true
		}
	}
	return model.KeyValue{}, false
}

func (m *Store) validSpan(span *model.Span, query *spanstore.TraceQueryParameters) bool {
	if query.ServiceName != span.Process.ServiceName {
		return false
	}
	if query.OperationName != "" && query.OperationName != span.OperationName {
		return false
	}
	if query.DurationMin != 0 && span.Duration < query.DurationMin {
		return false
	}
	if query.DurationMax != 0 && span.Duration > query.DurationMax {
		return false
	}
	if !query.StartTimeMin.IsZero() && span.StartTime.Before(query.StartTimeMin) {
		return false
	}
	if !query.StartTimeMax.IsZero() && span.StartTime.After(query.StartTimeMax) {
		return false
	}
	spanKVs := m.flattenTags(span)
	for queryK, queryV := range query.Tags {
		// (NB): we cannot use the KeyValues.FindKey function because there can be multiple tags with the same key
		if _, ok := findKeyValueMatch(spanKVs, queryK, queryV); !ok {
			return false
		}
	}
	return true
}

func (m *Store) flattenTags(span *model.Span) model.KeyValues {
	retMe := span.Tags
	retMe = append(retMe, span.Process.Tags...)
	for _, l := range span.Logs {
		retMe = append(retMe, l.Fields...)
	}
	return retMe
}
