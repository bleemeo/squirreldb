package ledis

import (
	"fmt"
	"strconv"

	"github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/ledis"

	"bytes"
	"encoding/binary"
	"squirreldb/compare"
	"squirreldb/types"
	"time"
)

const (
	metricKeyPrefix   = "squirreldb-metric-"
	offsetKeyPrefix   = "squirreldb-offset-"
	deadlineKeyPrefix = "squirreldb-flushdeadline-"
	knownMetricsKey   = "squirreldb-known-metrics"
	transfertKey      = "squirreldb-transfert-metrics"
)

type Ledis struct {
	ledis *ledis.Ledis
	db    *ledis.DB
}

type serializedPoints struct {
	Timestamp  int64
	Value      float64
	TimeToLive int64
}

const serializedSize = 24
const defaultTTL = 24 * time.Hour

// Init initialize the Ledis database
func (l *Ledis) Init() error {
	cfg := config.NewConfigDefault()

	tmp, err := ledis.Open(cfg)
	if err != nil {
		return err
	}

	l.ledis = tmp

	l.db, err = l.ledis.Select(0)

	return err
}

func (l *Ledis) Close() {
	l.ledis.Close()
}

func metricID2String(id types.MetricID) string {
	return strconv.FormatInt(int64(id), 36)
}

func string2MetricID(input string) (types.MetricID, error) {
	v, err := strconv.ParseInt(input, 36, 0)
	return types.MetricID(v), err
}

// Append implement batch.TemporaryStore interface
func (l *Ledis) Append(points []types.MetricData) ([]int, error) {
	start := time.Now()

	defer func() {
		operationSecondsAdd.Observe(time.Since(start).Seconds())
	}()

	if len(points) == 0 {
		return nil, nil
	}

	results := make([]int, len(points))

	var addedPoints int

	for i, data := range points {
		addedPoints += len(data.Points)
		values, err := valuesFromData(data)

		if err != nil {
			return nil, err
		}

		key := []byte(metricKeyPrefix + metricID2String(data.ID))

		tmp, err := l.db.Append(key, values)
		if err != nil {
			return nil, err
		}

		results[i] = int(tmp / serializedSize)
	}

	operationPointssAdd.Add(float64(addedPoints))

	return results, nil
}

// GetSetPointsAndOffset implement batch.TemporaryStore interface
func (l *Ledis) GetSetPointsAndOffset(points []types.MetricData, offsets []int) ([]types.MetricData, error) {
	start := time.Now()

	defer func() {
		operationSecondsSet.Observe(time.Since(start).Seconds())
	}()

	if len(points) == 0 {
		return nil, nil
	}

	if len(points) != len(offsets) {
		return nil, fmt.Errorf("GetSetPointsAndOffset: len(points) == %d must be equal to len(offsets) == %d", len(points), len(offsets))
	}

	results := make([]types.MetricData, len(points))
	ids := make([][]byte, len(points))

	var writtenPointsCount int

	for i, data := range points {
		writtenPointsCount += len(data.Points)
		values, err := valuesFromData(data)

		if err != nil {
			return nil, err
		}

		idStr := metricID2String(data.ID)
		metricKey := []byte(metricKeyPrefix + idStr)
		offsetKey := []byte(offsetKeyPrefix + idStr)

		tmp, err := l.db.GetSet(metricKey, values)
		if err != nil {
			return nil, err
		}

		if len(tmp) > 0 {
			results[i], err = dataFromValues(data.ID, tmp)
			if err != nil {
				return nil, err
			}
		}

		ids[i] = []byte(idStr)

		_, err = l.db.Expire(metricKey, int64(defaultTTL.Seconds()))
		if err != nil {
			return nil, err
		}

		err = l.db.SetEX(offsetKey, int64(defaultTTL.Seconds()), []byte(strconv.FormatInt(int64(offsets[i]), 10)))
		if err != nil {
			return nil, err
		}
	}

	_, err := l.db.SAdd([]byte(knownMetricsKey), ids...)
	if err != nil {
		return nil, err
	}

	operationPointssSet.Add(float64(writtenPointsCount))

	return results, nil
}

// ReadPointsAndOffset implement batch.TemporaryStore interface
func (l *Ledis) ReadPointsAndOffset(ids []types.MetricID) ([]types.MetricData, []int, error) { // nolint: gocognit
	start := time.Now()

	defer func() {
		operationSecondsGet.Observe(time.Since(start).Seconds())
	}()

	if len(ids) == 0 {
		return nil, nil, nil
	}

	metrics := make([]types.MetricData, len(ids))
	writeOffsets := make([]int, len(ids))

	var readPointsCount int

	for i, id := range ids {
		metricKey := []byte(metricKeyPrefix + metricID2String(id))
		offsetKey := []byte(offsetKeyPrefix + metricID2String(id))

		values, err := l.db.Get(metricKey)
		if err != nil {
			return nil, nil, err
		}

		if len(values) > 0 {
			metrics[i], err = dataFromValues(id, values)

			if err != nil {
				return nil, nil, err
			}

			readPointsCount += len(values) / serializedSize
		}

		tmp, err := l.db.Get(offsetKey)
		if err != nil {
			return nil, nil, err
		}

		writeOffsets[i], err = strconv.Atoi(string(tmp))
		if err != nil {
			return nil, nil, err
		}
	}

	operationPointssGet.Add(float64(readPointsCount))

	return metrics, writeOffsets, nil
}

// MarkToExpire implement batch.TemporaryStore interface
func (l *Ledis) MarkToExpire(ids []types.MetricID, ttl time.Duration) error {
	start := time.Now()

	defer func() {
		operationSecondsExpire.Observe(time.Since(start).Seconds())
	}()

	if len(ids) == 0 {
		return nil
	}

	idsStr := make([][]byte, len(ids))

	for i, id := range ids {
		idStr := metricID2String(id)
		metricKey := []byte(metricKeyPrefix + idStr)
		offsetKey := []byte(offsetKeyPrefix + idStr)
		deadlineKey := []byte(deadlineKeyPrefix + idStr)

		idsStr[i] = []byte(idStr)

		_, err := l.db.Expire(metricKey, int64(ttl.Seconds()))
		if err != nil {
			return err
		}

		_, err = l.db.Expire(offsetKey, int64(ttl.Seconds()))
		if err != nil {
			return err
		}

		_, err = l.db.Expire(deadlineKey, int64(ttl.Seconds()))
		if err != nil {
			return err
		}
	}

	_, err := l.db.SRem([]byte(knownMetricsKey), idsStr...)
	if err != nil {
		return err
	}

	return nil
}

// GetSetFlushDeadline implement batch.TemporaryStore interface
func (l *Ledis) GetSetFlushDeadline(deadlines map[types.MetricID]time.Time) (map[types.MetricID]time.Time, error) {
	start := time.Now()

	defer func() {
		operationSecondsSetDeadline.Observe(time.Since(start).Seconds())
	}()

	if len(deadlines) == 0 {
		return nil, nil
	}

	results := make(map[types.MetricID]time.Time, len(deadlines))

	for id, deadline := range deadlines {
		deadlineKey := []byte(deadlineKeyPrefix + metricID2String(id))

		tmp, err := l.db.GetSet(deadlineKey, []byte(deadline.Format(time.RFC3339)))
		if err != nil {
			return nil, err
		}

		if len(tmp) > 0 {
			results[id], err = time.Parse(time.RFC3339, string(tmp))
			if err != nil {
				return nil, err
			}
		}

		_, err = l.db.Expire(deadlineKey, int64(defaultTTL.Seconds()))
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// AddToTransfert implement batch.TemporaryStore interface
func (l *Ledis) AddToTransfert(ids []types.MetricID) error {
	if len(ids) == 0 {
		return nil
	}

	strings := make([][]byte, len(ids))

	for i, id := range ids {
		strings[i] = []byte(metricID2String(id))
	}

	_, err := l.db.SAdd([]byte(transfertKey), strings...)

	return err
}

// GetTransfert implement batch.TemporaryStore interface
func (l *Ledis) GetTransfert(count int) (map[types.MetricID]time.Time, error) {
	result, err := l.db.SScan([]byte(transfertKey), []byte("0"), count, true, "")
	if err != nil {
		return nil, err
	}

	if len(result) > 0 {
		_, err = l.db.SRem([]byte(transfertKey), result...)
		if err != nil {
			return nil, err
		}
	}

	return l.getFlushDeadline(result)
}

// GetAllKnownMetrics implement batch.TemporaryStore interface
func (l *Ledis) GetAllKnownMetrics() (map[types.MetricID]time.Time, error) {
	start := time.Now()

	defer func() {
		operationSecondsKnownMetrics.Observe(time.Since(start).Seconds())
	}()

	result, err := l.db.SMembers([]byte(knownMetricsKey))
	if err != nil {
		return nil, err
	}

	return l.getFlushDeadline(result)
}

func (l *Ledis) getFlushDeadline(ids [][]byte) (map[types.MetricID]time.Time, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	results := make(map[types.MetricID]time.Time, len(ids))

	for _, idStr := range ids {
		deadlineKey := append([]byte(deadlineKeyPrefix), idStr...)

		tmp, err := l.db.Get(deadlineKey)
		if err != nil {
			return nil, err
		}

		if len(tmp) > 0 {
			id, err := string2MetricID(string(idStr))
			if err != nil {
				return nil, err
			}

			results[id], err = time.Parse(time.RFC3339, string(tmp))

			if err != nil {
				return nil, err
			}
		}
	}

	return results, nil
}

// Return data from bytes values
func dataFromValues(id types.MetricID, values []byte) (types.MetricData, error) {
	data := types.MetricData{}
	buffer := bytes.NewReader(values)

	dataSerialized := make([]serializedPoints, len(values)/24)

	err := binary.Read(buffer, binary.BigEndian, &dataSerialized)
	if err != nil {
		return data, err
	}

	data.Points = make([]types.MetricPoint, len(dataSerialized))
	for i, point := range dataSerialized {
		data.ID = id
		data.Points[i] = types.MetricPoint{
			Timestamp: point.Timestamp,
			Value:     point.Value,
		}
		data.TimeToLive = compare.MaxInt64(data.TimeToLive, point.TimeToLive)
	}

	return data, nil
}

// Return bytes values from data
func valuesFromData(data types.MetricData) ([]byte, error) {
	buffer := new(bytes.Buffer)
	buffer.Grow(len(data.Points) * 24)

	dataSerialized := make([]serializedPoints, len(data.Points))
	for i, point := range data.Points {
		dataSerialized[i] = serializedPoints{
			Timestamp:  point.Timestamp,
			Value:      point.Value,
			TimeToLive: data.TimeToLive,
		}
	}

	if err := binary.Write(buffer, binary.BigEndian, dataSerialized); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}
