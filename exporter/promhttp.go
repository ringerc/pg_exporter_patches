package exporter

/*
The prometheus client library does not expose a way to modify the
HTTP handler to handle query parameters or other customizations.

To support selectively scraping different metric sets, we could use a custom
HTTP handler that wraps the promhttp.HandlerFor to create a handler for a set
of promhttp.Gatherers satisfying the requested query parameters. Each would
point to a different Registry that contains the metrics for that set. But the
current pg_exporter structure collects metrics imperatively so this isn't
possible - there's only one Collector as far as Prometheus is concerned,
and only one Registry entry.
*/

import (
	"fmt"
	"context"
	"strconv"
	"errors"
	"strings"
	"time"
	"net/http"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	scrapeTimeoutHeader = "X-Prometheus-Scrape-Timeout-Seconds"
)

type groupsContextKey struct{}

// wrapperHandler wraps http.Handler to wrap a promhttp.Handler
// for custom header handling and selective scrape.
type wrapperHandler struct {
	exporter *Exporter
}

// A prometheus.Collector used for one scrape. It binds the
// scrape-request-specific parameters and passes them to the
// Exporter.
type Scrape struct {
	exporter *Exporter
	ctx context.Context
}

// Describe is not context-aware
func (s Scrape) Describe(ch chan<- *prometheus.Desc) {
	s.exporter.Describe(ch)
}

func (s Scrape) Collect(ch chan<- prometheus.Metric) {
	s.exporter.CollectWithContext(s.ctx, ch)
}

// ServeHTTP implements http.Handler
//
// Invoke the promhttp.Handler after checking for selective scrape query
// parameters in the request.
//
func (h wrapperHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	// TODO respect the header scrape timeout. This is a bit tricky because
	// Prometheus APIs don't propagate Context through to the Gatherer and
	// Collector, and because context.WithTimeout doesn't nest when creating
	// sub-timeouts for specific queries.
	timeout := 0 * time.Second
	if v := r.Header.Get(scrapeTimeoutHeader); v != "" {
		timeoutSeconds, err := strconv.ParseFloat(v, 64)
		if err != nil {
			switch {
			case errors.Is(err, strconv.ErrSyntax):
				// fixme
				fmt.Printf("%s: unsupported value", scrapeTimeoutHeader)
			case errors.Is(err, strconv.ErrRange):
				// fixme
				fmt.Printf("%s: value out of range", scrapeTimeoutHeader)
			}
		} else {
			timeout = time.Duration(timeoutSeconds * float64(time.Second))
			// Timeout from the headers is not presently respected
			logDebugf("timeout from header %s ignored: %v", scrapeTimeoutHeader, timeout)
		}
	}

	// We can inject the scrape timeout into the context of the Request using
	// something like
	//    r.WithContext(context.WithDeadline(r.Context(), deadline))
	// but it won't be respected by the promhttp.Handler because it doesn't
	// propagate context through to the Gatherer and Collector.
	// See https://github.com/prometheus/client_golang/issues/1538
	//
	// The HTTP request state and the Context for it isn't accessible to the
	// Collector directly.
	//
	// We can work around this by:
    //
	// - instantiating a new Handler, Gatherer, Collector etc for each
	//   request; these must be decoupled from the longer lived
	//   Exporter, Server and Query objects; or
	// - Limiting requests to run serially and storing the context
	//   in the Exporter instance

	scrapeContext := r.Context()
	groups := r.URL.Query().Get("groups")
	if groups != "" {
		// Handle a request for a specific group of metrics
		// by creating a new Scrape object with the context
		// and passing it to the promhttp.Handler.
		logDebugf("scrape request for groups: %s", groups)
		scrapeContext = context.WithValue(scrapeContext, groupsContextKey{}, strings.Split(groups, ","))
	}

	scrape := Scrape{
		exporter: h.exporter,
		ctx: scrapeContext,
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(scrape)
	promhandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	// Delegate to the one-shot promhttp.Handler
	promhandler.ServeHTTP(w, r)
}

func newPrometheusHandler(exporter *Exporter) http.Handler {
	// Prepare a wrapper handler that delegates to a promhttp.Handler
	// for the correct query parameters.
	wrapperHandler := wrapperHandler{
		exporter: exporter,
	}
	// Wrap our custom handler with the promhttp.InstrumentMetricHandler
	// so we track results and concurrent requests even though we're
	// destroying the real Prometheus handler for every request.
	return promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer, wrapperHandler,
	) 
}
