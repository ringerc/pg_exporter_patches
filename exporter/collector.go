package exporter

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
	"time"
)

/**********************************************************************************************\
*                                   Query Instance                                             *
\**********************************************************************************************/

// Collector holds runtime information of a Query running on a Server
// It is deeply coupled with Server. Besides, it can be a collector itself
type Collector struct {
	*Query
	Server *Server // It's a query, but holds a server

	// runtime information
	lock        sync.RWMutex                // scrape lock
	result      []prometheus.Metric         // cached metrics
	descriptors map[string]*prometheus.Desc // maps column index to descriptor, build on init
	cacheHit    bool                        // indicate last scrape was served from cache or real execution
	err         error

	// stats
	lastScrape     time.Time     // SERVER's scrape start time (for cache window align)
	scrapeBegin    time.Time     // execution begin time
	scrapeDone     time.Time     // execution complete time
	scrapeDuration time.Duration // last real execution duration
}

// NewCollector will generate query instance from query, Injecting a server object
func NewCollector(q *Query, s *Server) *Collector {
	instance := &Collector{
		Query:  q,
		Server: s,
		result: make([]prometheus.Metric, 0),
	}
	instance.makeDescMap()
	return instance
}

// Describe implement prometheus.Collector
func (q *Collector) Describe(ch chan<- *prometheus.Desc) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.sendDescriptors(ch)
}

func (q *Collector) CollectWithContext(ctx context.Context, ch chan<- prometheus.Metric) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.scrapeBegin = time.Now()
	if q.cacheExpired() || q.Server.DisableCache {
		q.execute(ctx)
		q.cacheHit = false
		q.scrapeDone = time.Now()
		q.scrapeDuration = q.scrapeDone.Sub(q.scrapeBegin)
		q.lastScrape = q.Server.scrapeBegin
	} else { // serve from cache
		q.cacheHit = true
		q.scrapeDone = time.Now()
	}
	q.sendMetrics(ch) // the cache is already reset to zero even execute failed
}

// This shouldn't actually be invoked directly
func (q *Collector) Collect(ch chan<- prometheus.Metric) {
	q.CollectWithContext(context.Background(), ch)
}

// ResultSize report last scrapped metric count
func (q *Collector) ResultSize() int {
	return len(q.result)
}

// Error wraps query error
func (q *Collector) Error() error {
	return q.err
}

// Duration returns last scrape duration in float64 seconds
func (q *Collector) Duration() float64 {
	return q.scrapeDone.Sub(q.scrapeBegin).Seconds()
}

// CacheHit report whether last scrape was serve from cache
func (q *Collector) CacheHit() bool {
	return q.cacheHit
}

// execute will run this query to registered server, result and err are registered
func (q *Collector) execute(ctx context.Context) {
	q.result = q.result[:0] // reset cache
	var rows *sql.Rows
	var err error

	// execution
	if q.Timeout != 0 { // if timeout is provided, use context
		logDebugf("query [%s] @ server [%s] executing begin with time limit: %v", q.Name, q.Server.Database, q.TimeoutDuration())
		// TODO should use WithDeadline here so it properly handles parent context deadline
		ctx, cancel := context.WithTimeout(ctx, q.TimeoutDuration())
		defer cancel()
		rows, err = q.Server.QueryContext(ctx, q.SQL)
	} else {
		logDebugf("query [%s] executing begin", q.Name)
		rows, err = q.Server.Query(q.SQL)
	}

	// error handling: if query failed because of timeout or error, record and return
	if err != nil {
		if err == context.DeadlineExceeded { // timeout
			q.err = fmt.Errorf("query [%s] timeout because duration %v exceed limit %v",
				q.Name, time.Now().Sub(q.scrapeBegin), q.TimeoutDuration())
		} else {
			q.err = fmt.Errorf("query [%s] failed: %w", q.Name, err)
		}
		return
	}
	defer rows.Close()

	// parsing meta:  fetch column metadata for dynamic name lookup
	columnNames, err := rows.Columns()
	if err != nil {
		q.err = fmt.Errorf("query [%s] fail retriving rows meta: %w", q.Name, err)
		return
	}
	columnIndexes := make(map[string]int, len(columnNames)) // column name to index
	for i, n := range columnNames {
		columnIndexes[n] = i
	}
	nColumn := len(columnNames)
	colData := make([]interface{}, nColumn)
	colArgs := make([]interface{}, nColumn)
	for i := range colData {
		colArgs[i] = &colData[i]
	}
	if len(columnNames) != len(q.Columns) { // warn if column count not match
		logWarnf("query [%s] column count not match, result %d ≠ config %d", q.Name, len(columnNames), len(q.Columns))
	}

	// scan loop: for each row, extract labels from all label columns, then generate a new metric for each metric column
	for rows.Next() {
		err = rows.Scan(colArgs...)
		if err != nil {
			q.err = fmt.Errorf("fail scanning rows: %w", err)
			return
		}

		// get labels, sequence matters, empty string for null or bad labels
		labels := make([]string, len(q.LabelNames))
		for i, labelName := range q.LabelNames {
			if dataIndex, found := columnIndexes[labelName]; found {
				labels[i] = castString(colData[dataIndex])
			} else {
				//if label column is not found in result, we just warn and send a empty string
				logWarnf("missing label %s.%s", q.Name, labelName)
				labels[i] = ""
			}
		}

		// get metrics, warn if column not exist
		for _, metricName := range q.MetricNames {
			if dataIndex, found := columnIndexes[metricName]; found { // the metric column is found in result
				q.result = append(q.result,
					prometheus.MustNewConstMetric(
						q.descriptors[metricName], // always find desc & column via name
						q.Columns[metricName].PrometheusValueType(),
						castFloat64(colData[dataIndex], q.Columns[metricName].Scale, q.Columns[metricName].Default),
						labels...,
					))
			} else {
				logWarnf("missing metric column %s.%s in result", q.Name, metricName)
			}
		}
	}
	q.err = nil
	logDebugf("query [%s] executing complete in %v, metrics count: %d",
		q.Name, time.Now().Sub(q.scrapeBegin), len(q.result))
	return
}

/**************************************************************\
* Query Instance Auxiliary
\**************************************************************/

// makeDescMap will generate descriptor map from Query
func (q *Collector) makeDescMap() {
	descriptors := make(map[string]*prometheus.Desc, 0)

	// rename label name if label column have rename option
	labelNames := make([]string, len(q.LabelNames))
	for i, labelName := range q.LabelNames {
		labelColumn := q.Columns[labelName]
		if labelColumn.Rename != "" {
			labelNames[i] = labelColumn.Rename
		} else {
			labelNames[i] = labelColumn.Name
		}
	}

	// rename metric if metric column have a rename option
	for _, metricName := range q.MetricNames {
		metricColumn := q.Columns[metricName] // always found
		metricName := fmt.Sprintf("%s_%s", q.Name, metricColumn.Name)
		if metricColumn.Rename != "" {
			metricName = fmt.Sprintf("%s_%s", q.Name, metricColumn.Rename)
		}
		descriptors[metricColumn.Name] = prometheus.NewDesc(
			metricName, metricColumn.Desc, labelNames, q.Server.labels,
		)
	}
	q.descriptors = descriptors
}

func (q *Collector) sendDescriptors(ch chan<- *prometheus.Desc) {
	for _, desc := range q.descriptors {
		ch <- desc
	}
}

// cacheExpired report whether this instance needs actual execution
// Note you have to using Server.scrapeBegin as "now", and set that timestamp as
func (q *Collector) cacheExpired() bool {
	if q.Server.scrapeBegin.Sub(q.lastScrape) > time.Duration(q.TTL*float64(time.Second)) {
		return true
	}
	return false
}

func (q *Collector) cacheTTL() float64 {
	return q.TTL - q.Server.scrapeBegin.Sub(q.lastScrape).Seconds()
}

// sendMetrics will send cached result to ch
func (q *Collector) sendMetrics(ch chan<- prometheus.Metric) {
	for _, metric := range q.result {
		ch <- metric
	}
}
