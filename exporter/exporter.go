package exporter

import (
	"fmt"
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"sync"
	"time"
)

/**********************************************************************************************\
*                                        Exporter                                              *
\**********************************************************************************************/

// Exporter implement prometheus.Collector interface
// exporter contains one or more (auto-discover-database) servers that can scrape metrics with Query
type Exporter struct {
	// config params provided from ExporterOpt
	dsn             string            // primary dsn
	configPath      string            // config file path /directory
	disableCache    bool              // always execute query when been scrapped
	disableIntro    bool              // disable query level introspection metrics
	autoDiscovery   bool              // discovery other database on primary server
	pgbouncerMode   bool              // is primary server a pgbouncer ?
	failFast        bool              // fail fast instead fof waiting during start-up ?
	excludeDatabase map[string]bool   // excluded database for auto discovery
	includeDatabase map[string]bool   // include database for auto discovery
	constLabels     prometheus.Labels // prometheus const k=v labels
	tags            []string          // tags passed to this exporter for scheduling purpose
	namespace       string            // metrics prefix ('pg' or 'pgbouncer' by default)
	connectTimeout  int               // timeout in ms when perform server pre-check

	// internal status
	lock    sync.RWMutex       // export lock
	server  *Server            // primary server
	sLock   sync.RWMutex       // server map lock
	servers map[string]*Server // auto discovered peripheral servers
	queries map[string]*Query  // metrics query definition

	// internal stats
	scrapeBegin time.Time // server level scrape begin
	scrapeDone  time.Time // server last scrape done

	// internal metrics: global, exporter, server, query
	up               prometheus.Gauge   // cluster level: primary target server is alive
	version          prometheus.Gauge   // cluster level: postgres main server version num
	recovery         prometheus.Gauge   // cluster level: postgres is in recovery ?
	exporterUp       prometheus.Gauge   // exporter level: always set ot 1
	exporterUptime   prometheus.Gauge   // exporter level: primary target server uptime (exporter itself)
	lastScrapeTime   prometheus.Gauge   // exporter level: last scrape timestamp
	scrapeDuration   prometheus.Gauge   // exporter level: seconds spend on scrape
	scrapeTotalCount prometheus.Counter // exporter level: total scrape count of this server
	scrapeErrorCount prometheus.Counter // exporter level: error scrape count

	serverScrapeDuration     *prometheus.GaugeVec // {datname} database level: how much time spend on server scrape?
	serverScrapeTotalSeconds *prometheus.GaugeVec // {datname} database level: how much time spend on server scrape?
	serverScrapeTotalCount   *prometheus.GaugeVec // {datname} database level how many metrics scrapped from server
	serverScrapeErrorCount   *prometheus.GaugeVec // {datname} database level: how many error occurs when scrapping server

	queryCacheTTL          *prometheus.GaugeVec // {datname,query} query cache ttl
	queryScrapeTotalCount  *prometheus.GaugeVec // {datname,query} query level: how many errors the query triggers?
	queryScrapeErrorCount  *prometheus.GaugeVec // {datname,query} query level: how many errors the query triggers?
	queryScrapeDuration    *prometheus.GaugeVec // {datname,query} query level: how many seconds the query spends?
	queryScrapeMetricCount *prometheus.GaugeVec // {datname,query} query level: how many metrics the query returns?
	queryScrapeHitCount    *prometheus.GaugeVec // {datname,query} query level: how many errors the query triggers?

}

// Up will delegate aliveness check to primary server
func (e *Exporter) Up() bool {
	return e.server.UP
}

// Recovery will delegate primary/replica check to primary server
func (e *Exporter) Recovery() bool {
	return e.server.Recovery
}

// Status will report 4 available status: primary|replica|down|unknown
func (e *Exporter) Status() string {
	if e.server == nil {
		return `unknown`
	}
	if !e.server.UP {
		return `down`
	} else {
		if e.server.Recovery {
			return `replica`
		} else {
			return `primary`
		}
	}
}

// Describe implement prometheus.Collector
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	e.server.Describe(ch)
}

func (e *Exporter) CollectWithContext(ctx context.Context, ch chan<- prometheus.Metric) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.scrapeTotalCount.Add(1)

	groups, ok := ctx.Value(groupsContextKey{}).([]string)
	if ok {
		logDebugf("scrape groups in server: %v", groups)
	} else {
		logDebugf("scrape groups in server: all")
	}

	e.scrapeBegin = time.Now()
	// scrape primary server
	s := e.server
	s.CollectWithContext(ctx, ch)

	// scrape extra servers if exists
	// This could be parallelized into goroutines to better handle scrapes
	// of many DBs, probably by using a sync.WaitGroup or putting each in
	// a separate Registry then scraping them with prometheus.Gather.
	for _, srv := range e.IterateServer() {
		srv.CollectWithContext(ctx, ch)
	}
	e.scrapeDone = time.Now()

	e.lastScrapeTime.Set(float64(e.scrapeDone.Unix()))
	e.scrapeDuration.Set(e.scrapeDone.Sub(e.scrapeBegin).Seconds())
	e.version.Set(float64(s.Version))
	if s.UP {
		e.up.Set(1)
		if s.Recovery {
			e.recovery.Set(1)
		} else {
			e.recovery.Set(0)
		}
	} else {
		e.up.Set(0)
		e.scrapeErrorCount.Add(1)
	}
	e.exporterUptime.Set(e.server.Uptime())
	e.collectServerMetrics(s)
	e.collectInternalMetrics(ch)
}

// Collect implement prometheus.Collector
//
// This is the top level Collector that's actually registered with Prometheus.
// It delegates to each Server.
//
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	e.CollectWithContext(context.Background(), ch)
}

func (e *Exporter) collectServerMetrics(s *Server) {
	e.serverScrapeDuration.Reset()
	e.serverScrapeTotalSeconds.Reset()
	e.serverScrapeTotalCount.Reset()
	e.serverScrapeErrorCount.Reset()
	e.queryCacheTTL.Reset()
	e.queryScrapeTotalCount.Reset()
	e.queryScrapeErrorCount.Reset()
	e.queryScrapeDuration.Reset()
	e.queryScrapeMetricCount.Reset()
	e.queryScrapeHitCount.Reset()

	servers := e.IterateServer()
	servers = append(servers, e.server) // append primary server to extra server list
	for _, s := range servers {
		e.serverScrapeDuration.WithLabelValues(s.Database).Set(s.Duration())
		e.serverScrapeTotalSeconds.WithLabelValues(s.Database).Set(s.totalTime)
		e.serverScrapeTotalCount.WithLabelValues(s.Database).Set(s.totalCount)
		if s.Error() != nil {
			e.serverScrapeErrorCount.WithLabelValues(s.Database).Add(1)
		}

		for queryName, counter := range s.queryCacheTTL {
			e.queryCacheTTL.WithLabelValues(s.Database, queryName).Set(counter)
		}
		for queryName, counter := range s.queryScrapeTotalCount {
			e.queryScrapeTotalCount.WithLabelValues(s.Database, queryName).Set(counter)
		}
		for queryName, counter := range s.queryScrapeHitCount {
			e.queryScrapeHitCount.WithLabelValues(s.Database, queryName).Set(counter)
		}
		for queryName, counter := range s.queryScrapeErrorCount {
			e.queryScrapeErrorCount.WithLabelValues(s.Database, queryName).Set(counter)
		}
		for queryName, counter := range s.queryScrapeMetricCount {
			e.queryScrapeMetricCount.WithLabelValues(s.Database, queryName).Set(counter)
		}
		for queryName, counter := range s.queryScrapeDuration {
			e.queryScrapeDuration.WithLabelValues(s.Database, queryName).Set(counter)
		}
	}
}

// Explain is just yet another wrapper of server.ExplainHTML
func (e *Exporter) Explain() string {
	return e.server.Explain()
}

// Stat is just yet another wrapper of server.Stat
func (e *Exporter) Stat() string {
	logDebugf("stats invoked")
	return e.server.Stat()
}

// Check will perform an immediate server health check
func (e *Exporter) Check() {
	if err := e.server.Check(); err != nil {
		logErrorf("exporter check failure: %s", err.Error())
	} else {
		logDebugf("exporter check ok")
	}
}

// Close will close all underlying servers
func (e *Exporter) Close() {
	if e.server != nil {
		err := e.server.Close()
		if err != nil {
			logErrorf("fail closing server %s: %s", e.server.Name(), err.Error())
		}
	}
	// close peripheral servers (we may skip acquire lock here)
	for _, srv := range e.IterateServer() {
		err := srv.Close()
		if err != nil {
			logErrorf("fail closing server %s: %s", e.server.Name(), err.Error())
		}
	}
	logInfof("pg exporter closed")
}

// setupInternalMetrics will init internal metrics
func (e *Exporter) setupInternalMetrics() {
	if e.namespace == "" {
		if e.pgbouncerMode {
			e.namespace = "pgbouncer"
		} else {
			e.namespace = "pg"
		}
	}

	// major fact
	e.up = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Name: "up", Help: "last scrape was able to connect to the server: 1 for yes, 0 for no",
	})
	e.version = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Name: "version", Help: "server version number",
	})
	e.recovery = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Name: "in_recovery", Help: "server is in recovery mode? 1 for yes 0 for no",
	})

	// exporter level metrics
	e.exporterUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter", Name: "up", Help: "always be 1 if your could retrieve metrics",
	})
	e.exporterUptime = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter", Name: "uptime", Help: "seconds since exporter primary server inited",
	})
	e.scrapeTotalCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter", Name: "scrape_total_count", Help: "times exporter was scraped for metrics",
	})
	e.scrapeErrorCount = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter", Name: "scrape_error_count", Help: "times exporter was scraped for metrics and failed",
	})
	e.scrapeDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter", Name: "scrape_duration", Help: "seconds exporter spending on scrapping",
	})
	e.lastScrapeTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter", Name: "last_scrape_time", Help: "seconds exporter spending on scrapping",
	})

	// exporter level metrics
	e.serverScrapeDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter_server", Name: "scrape_duration", Help: "seconds exporter server spending on scrapping",
	}, []string{"datname"})
	e.serverScrapeTotalSeconds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter_server", Name: "scrape_total_seconds", Help: "seconds exporter server spending on scrapping",
	}, []string{"datname"})
	e.serverScrapeTotalCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter_server", Name: "scrape_total_count", Help: "times exporter server was scraped for metrics",
	}, []string{"datname"})
	e.serverScrapeErrorCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter_server", Name: "scrape_error_count", Help: "times exporter server was scraped for metrics and failed",
	}, []string{"datname"})

	// query level metrics
	e.queryCacheTTL = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter_query", Name: "cache_ttl", Help: "times to live of query cache",
	}, []string{"datname", "query"})
	e.queryScrapeTotalCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter_query", Name: "scrape_total_count", Help: "times exporter server was scraped for metrics",
	}, []string{"datname", "query"})
	e.queryScrapeErrorCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter_query", Name: "scrape_error_count", Help: "times the query failed",
	}, []string{"datname", "query"})
	e.queryScrapeDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter_query", Name: "scrape_duration", Help: "seconds query spending on scrapping",
	}, []string{"datname", "query"})
	e.queryScrapeMetricCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter_query", Name: "scrape_metric_count", Help: "numbers of metrics been scrapped from this query",
	}, []string{"datname", "query"})
	e.queryScrapeHitCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: e.namespace, ConstLabels: e.constLabels,
		Subsystem: "exporter_query", Name: "scrape_hit_count", Help: "numbers been scrapped from this query",
	}, []string{"datname", "query"})

	e.exporterUp.Set(1) // always be true
}

func (e *Exporter) collectInternalMetrics(ch chan<- prometheus.Metric) {
	ch <- e.up
	ch <- e.version
	ch <- e.recovery

	ch <- e.exporterUp
	ch <- e.exporterUptime
	ch <- e.lastScrapeTime
	ch <- e.scrapeTotalCount
	ch <- e.scrapeErrorCount
	ch <- e.scrapeDuration

	e.serverScrapeDuration.Collect(ch)
	e.serverScrapeTotalSeconds.Collect(ch)
	e.serverScrapeTotalCount.Collect(ch)
	e.serverScrapeErrorCount.Collect(ch)

	e.queryCacheTTL.Collect(ch)
	e.queryScrapeTotalCount.Collect(ch)
	e.queryScrapeErrorCount.Collect(ch)
	e.queryScrapeDuration.Collect(ch)
	e.queryScrapeMetricCount.Collect(ch)
	e.queryScrapeHitCount.Collect(ch)
}

/**************************************************************\
* Exporter Creation
\**************************************************************/

// NewExporter construct a PG Exporter instance for given dsn
func NewExporter(dsn string, opts ...ExporterOpt) (e *Exporter, err error) {
	e = &Exporter{dsn: dsn}
	e.servers = make(map[string]*Server, 0)
	for _, opt := range opts {
		opt(e)
	}

	if e.queries, err = LoadConfig(e.configPath); err != nil {
		return nil, fmt.Errorf("fail loading config file %s: %w", e.configPath, err)
	}
	logDebugf("exporter init with %d queries", len(e.queries))

	// note here the server is still not connected. it will trigger connecting when being scrapped
	e.server = NewServer(
		dsn,
		WithQueries(e.queries),
		WithConstLabel(e.constLabels),
		WithCachePolicy(e.disableCache),
		WithServerTags(e.tags),
		WithServerConnectTimeout(e.connectTimeout),
	)

	// register db change callback
	if e.autoDiscovery {
		logInfof("auto discovery is enabled, excludeDatabase=%v, includeDatabase=%v", e.excludeDatabase, e.includeDatabase)
		e.server.onDatabaseChange = e.OnDatabaseChange
	}

	logDebugf("check primary server connectivity")
	// check server immediately, will hang/exit according to failFast
	if err = e.server.Check(); err != nil {
		if !e.failFast {
			logErrorf("fail connecting to primary server: %s, retrying in 10s", err.Error())
			for err != nil {
				time.Sleep(10 * time.Second)
				if err = e.server.Check(); err != nil {
					logErrorf("fail connecting to primary server: %s, retrying in 10s", err.Error())
				}
			}
		} else {
			logErrorf("fail connecting to primary server: %s, exit", err.Error())
		}
	}
	if err != nil {

		e.server.Plan()
	}
	e.pgbouncerMode = e.server.PgbouncerMode
	e.setupInternalMetrics()

	return
}

// OnDatabaseChange will spawn new Server when new database is created
// and destroy Server if corresponding database is dropped
func (e *Exporter) OnDatabaseChange(change map[string]bool) {
	for dbname, add := range change {
		verb := "del"
		if add {
			verb = "add"
		}

		if dbname == e.server.Database {
			continue // skip primary database change
		}
		if _, found := e.excludeDatabase[dbname]; found {
			logInfof("skip database change: %v %v according to in excluded database list", verb, dbname)
			continue // skip exclude databases changes
		}
		if len(e.includeDatabase) > 0 {
			if _, found := e.includeDatabase[dbname]; !found {
				logInfof("skip database change: %v %v according to not in include database list", verb, dbname)
				continue // skip non-include databases changes
			}
		}
		if add {
			// TODO: spawn new server
			e.CreateServer(dbname)
		} else {
			// TODO: close old server
			e.RemoveServer(dbname)
		}
	}
}

// CreateServer will spawn new database server from a database name combined with existing dsn string
// This happens when a database is newly created
func (e *Exporter) CreateServer(dbname string) {
	newDSN := ReplaceDatname(e.dsn, dbname)
	logInfof("spawn new server for database %s : %s", dbname, ShadowPGURL(newDSN))
	newServer := NewServer(
		newDSN,
		WithQueries(e.queries),
		WithConstLabel(e.constLabels),
		WithCachePolicy(e.disableCache),
		WithServerTags(e.tags),
		WithServerConnectTimeout(e.connectTimeout),
	)
	newServer.Forked = true // important!

	e.sLock.Lock()
	e.servers[dbname] = newServer
	logInfof("database %s is installed due to auto-discovery", dbname)
	defer e.sLock.Unlock()
}

// RemoveServer will destroy Server from Exporter according to database name
// This happens when a database is dropped
func (e *Exporter) RemoveServer(dbname string) {
	e.sLock.Lock()
	delete(e.servers, dbname)
	logWarnf("database %s is removed due to auto-discovery", dbname)
	e.sLock.Unlock()
}

// IterateServer will get snapshot of extra servers
func (e *Exporter) IterateServer() (res []*Server) {
	if len(e.servers) > 0 {
		e.sLock.RLock()
		defer e.sLock.RUnlock()
		for _, srv := range e.servers {
			res = append(res, srv)
		}
	}
	return
}

// ExporterOpt configures Exporter
type ExporterOpt func(*Exporter)

// WithConfig add config path to Exporter
func WithConfig(configPath string) ExporterOpt {
	return func(e *Exporter) {
		e.configPath = configPath
	}
}

// WithConstLabels add const label to exporter. 0 length label returns nil
func WithConstLabels(s string) ExporterOpt {
	return func(e *Exporter) {
		e.constLabels = parseConstLabels(s)
	}
}

// WithCacheDisabled set cache param to exporter
func WithCacheDisabled(disableCache bool) ExporterOpt {
	return func(e *Exporter) {
		e.disableCache = disableCache
	}
}

// WithIntroDisabled will pass introspection option to server
func WithIntroDisabled(disableIntro bool) ExporterOpt {
	return func(s *Exporter) {
		s.disableIntro = disableIntro
	}
}

// WithFailFast marks exporter fail instead of waiting during start-up
func WithFailFast(failFast bool) ExporterOpt {
	return func(e *Exporter) {
		e.failFast = failFast
	}
}

// WithNamespace will specify metric namespace, by default is pg or pgbouncer
func WithNamespace(namespace string) ExporterOpt {
	return func(e *Exporter) {
		e.namespace = namespace
	}
}

// WithTags will register given tags to Exporter and all belonged servers
func WithTags(tags string) ExporterOpt {
	return func(e *Exporter) {
		e.tags = parseCSV(tags)
	}
}

// WithAutoDiscovery configures exporter with excluded database
func WithAutoDiscovery(flag bool) ExporterOpt {
	return func(e *Exporter) {
		e.autoDiscovery = flag
	}
}

// WithExcludeDatabase configures exporter with excluded database
func WithExcludeDatabase(excludeStr string) ExporterOpt {
	return func(e *Exporter) {
		exclMap := make(map[string]bool)
		exclList := parseCSV(excludeStr)
		for _, item := range exclList {
			exclMap[item] = true
		}
		e.excludeDatabase = exclMap
	}
}

// WithIncludeDatabase configures exporter with excluded database
func WithIncludeDatabase(includeStr string) ExporterOpt {
	return func(e *Exporter) {
		inclMap := make(map[string]bool)
		inclList := parseCSV(includeStr)
		for _, item := range inclList {
			inclMap[item] = true
		}
		e.includeDatabase = inclMap
	}
}

// WithConnectTimeout will specify timeout for conn pre-check.
// It's useful to increase this value when monitoring a remote instance (cross DC, cross AZ)
func WithConnectTimeout(timeout int) ExporterOpt {
	return func(e *Exporter) {
		e.connectTimeout = timeout
	}
}

/**************************************************************\
* Exporter REST API
\**************************************************************/
// ExplainFunc expose explain document
func (e *Exporter) ExplainFunc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	_, _ = w.Write([]byte(e.Explain()))
}

// StatFunc expose html statistics
func (e *Exporter) StatFunc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	_, _ = w.Write([]byte(e.Stat()))
}

// UpCheckFunc tells whether target instance is alive, 200 up 503 down
func (e *Exporter) UpCheckFunc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	e.Check()
	if e.Up() {
		w.WriteHeader(200)
		_, _ = w.Write([]byte(PgExporter.Status()))
	} else {
		w.WriteHeader(503)
		_, _ = w.Write([]byte(PgExporter.Status()))
	}
}

// PrimaryCheckFunc tells whether target instance is a primary, 200 yes 404 no 503 unknown
func (e *Exporter) PrimaryCheckFunc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	e.Check()
	if PgExporter.Up() {
		if PgExporter.Recovery() {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(PgExporter.Status()))
		} else {
			w.WriteHeader(200)
			_, _ = w.Write([]byte(PgExporter.Status()))
		}
	} else {
		w.WriteHeader(503)
		_, _ = w.Write([]byte(PgExporter.Status()))
	}
}

// ReplicaCheckFunc tells whether target instance is a replica, 200 yes 404 no 503 unknown
func (e *Exporter) ReplicaCheckFunc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	e.Check()
	if PgExporter.Up() {
		if PgExporter.Recovery() {
			w.WriteHeader(200)
			_, _ = w.Write([]byte(PgExporter.Status()))
		} else {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(PgExporter.Status()))
		}
	} else {
		w.WriteHeader(503)
		_, _ = w.Write([]byte(PgExporter.Status()))
	}
}

// VersionFunc responding current pg_exporter version
func VersionFunc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	payload := fmt.Sprintf("version %s", Version)
	_, _ = w.Write([]byte(payload))
}

// TitleFunc responding a description message
func TitleFunc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	_, _ = w.Write([]byte(`<html><head><title>PG Exporter</title></head><body><h1>PG Exporter</h1><p><a href='` + *metricPath + `'>Metrics</a></p></body></html>`))
}

// ReloadFunc handles reload request
func ReloadFunc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	if err := Reload(); err != nil {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(fmt.Sprintf("fail to reload: %s", err.Error())))
	} else {
		_, _ = w.Write([]byte(`server reloaded`))
	}
}
