package exporter

import (
	"bytes"
	"fmt"
	"gopkg.in/yaml.v3"
	"text/template"
	"time"
	"strings"
)

/**********************************************************************************************\
*                                       Query                                                  *
\**********************************************************************************************/

// Query hold the information of how to fetch metric and parse them
type Query struct {
	Name   string `yaml:"name"`  // actual query name, used as metric prefix
	Desc   string `yaml:"desc"`  // description of this metric query
	SQL    string `yaml:"query"` // SQL command to fetch metrics
	Branch string `yaml:"-"`     // branch name, top layer key of config file

	// control query behaviour
	Tags       []string `yaml:"tags"`               // tags are used for execution control
	TTL        float64  `yaml:"ttl"`                // caching ttl in seconds
	Timeout    float64  `yaml:"timeout"`            // query execution timeout in seconds
	Priority   int      `yaml:"priority,omitempty"` // execution priority, from 1 to 999
	MinVersion int      `yaml:"min_version"`        // minimal supported version, include
	MaxVersion int      `yaml:"max_version"`        // maximal supported version, not include
	Fatal      bool     `yaml:"fatal"`              // if query marked fatal fail, entire scrape will fail
	Skip       bool     `yaml:"skip"`               // if query marked skip, it will be omit while loading

	Metrics []map[string]*Column `yaml:"metrics"` // metric definition list

	// metrics parsing auxiliaries
	Path        string             `yaml:"-"` // where am I from ?
	Columns     map[string]*Column `yaml:"-"` // column map
	ColumnNames []string           `yaml:"-"` // column names in origin orders
	LabelNames  []string           `yaml:"-"` // column (name) that used as label, sequences matters
	MetricNames []string           `yaml:"-"` // column (name) that used as metric
}

var queryTemplate, _ = template.New("Query").Parse(`##
# SYNOPSIS
#       {{ .Name }}{{ if ne .Name .Branch }}.{{ .Branch }}{{ end }}_*
#
# DESCRIPTION
#       {{ with .Desc }}{{ . }}{{ else }}N/A{{ end }}
#
# OPTIONS
#       Tags       [{{ range $i, $e := .Tags }}{{ if $i }}, {{ end }}{{ $e }}{{ end }}]
#       TTL        {{ .TTL }}
#       Priority   {{ .Priority }}
#       Timeout    {{ .TimeoutDuration }}
#       Fatal      {{ .Fatal }}
#       Version    {{ if ne .MinVersion 0 }}{{ .MinVersion }}{{ else }}lower{{ end }} ~ {{ if ne .MaxVersion 0 }}{{ .MaxVersion }}{{ else }}higher{{ end }}
#       Source     {{ .Path }}
#
# METRICS
{{- range .ColumnList }}
#       {{ .Name }} ({{ .Usage }})
#           {{ with .Desc }}{{ . }}{{ else }}N/A{{ end }}{{ end }}
#
{{.MarshalYAML -}}
`)

var htmlTemplate, _ = template.New("Query").Parse(`
<div style="border-style: solid; padding-left: 20px; padding-bottom: 10px;">

<h2>{{ .Name }}</h2>
<p>{{ .Desc }}</p>

<h4>Query</h4>
<code><pre>{{ .SQL }}</pre></code>

<h4>Attribution</h4>
<code><table style="border-style: dotted;"><tbdoy>
<tr><td>Branch   </td> <td> {{ .Branch }} </td></tr>
<tr><td>TTL      </td> <td> {{ .TTL }} </td></tr>
<tr><td>Priority </td> <td> {{ .Priority }} </td></tr>
<tr><td>Timeout  </td> <td> {{ .TimeoutDuration }} </td></tr>
<tr><td>Fatal    </td> <td> {{ .Fatal }} </td></tr>
<tr><td>Version  </td> <td> {{if ne .MinVersion 0}}{{ .MinVersion }}{{else}}lower{{end}} ~ {{if ne .MaxVersion 0}}{{ .MaxVersion }}{{else}}higher{{end}} </td></tr>
<tr><td>Tags     </td> <td> {{ .Tags }} </td></tr>
<tr><td>Source   </td> <td> {{ .Path }} </td></tr>
<tbdoy></table></code>

<h4>Columns</h4>
<code><table "align="left"  style="border-style: dotted;"><thead><th>Name</th> <th>Usage</th> <th>Rename</th> <th>Bucket</th> <th>Scale</th> <th>Default</th> <th>Description</th></thead>
<tbdoy>{{ range .ColumnList }}<tr><td>{{ .Name }}</td><td>{{ .Usage }}</td><td>{{ .Rename }}</td><td>{{ .Bucket }}</td><td>{{ .Scale }}</td><td>{{ .Default }}</td><td>{{ .Desc }}</td></tr>{{ end }}
<tbdoy></table></code>

<h4>Metrics</h4>
<code><table "align="left"  style="border-style: dotted;"><thead><th>Name</th> <th>Usage</th> <th>Desc</th></th></thead><tbdoy>
{{ range .MetricList }}<tr><td>{{ .Name }}</td><td>{{ .Column.Usage }}</td><td>{{ .Column.Desc }}</td></tr>{{ end }}
<tbdoy></table></code>
</div>
`)

// MarshalYAML will turn query into YAML format
func (q *Query) MarshalYAML() string {
	// buf := new(bytes.Buffer)
	v := make(map[string]Query, 1)
	v[q.Branch] = *q
	buf, err := yaml.Marshal(v)
	if err != nil {
		msg := fmt.Sprintf("fail to marshall query yaml: %s", err.Error())
		logError(msg)
		return msg
	}
	return string(buf)
}

// Explain will turn query into text format
func (q *Query) Explain() string {
	buf := new(bytes.Buffer)
	err := queryTemplate.Execute(buf, q)
	if err != nil {
		msg := fmt.Sprintf("fail to explain query: %s", err.Error())
		logError(msg)
		return msg
	}
	return buf.String()
}

// HTML will turn Query into HTML format
func (q *Query) HTML() string {
	buf := new(bytes.Buffer)
	err := htmlTemplate.Execute(buf, q)
	if err != nil {
		msg := fmt.Sprintf("fail to generate query html: %s", err.Error())
		logError(msg)
		return msg
	}
	return buf.String()
}

// HasTag tells whether this query have specific tag
// since only few tags is provided, we don't really need a map here
func (q *Query) HasTag(tag string) bool {
	for _, t := range q.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// ColumnList return ordered column list
func (q *Query) ColumnList() (res []*Column) {
	res = make([]*Column, len(q.ColumnNames))
	for i, colName := range q.ColumnNames {
		res[i] = q.Columns[colName]
	}
	return
}

// LabelList returns a list of label column names
func (q *Query) LabelList() []string {
	labelNames := make([]string, len(q.LabelNames))
	for i, labelName := range q.LabelNames {
		labelColumn := q.Columns[labelName]
		if labelColumn.Rename != "" {
			labelNames[i] = labelColumn.Rename
		} else {
			labelNames[i] = labelColumn.Name
		}
	}
	return labelNames
}

// MetricList returns a list of MetricDesc generated by this query
func (q *Query) MetricList() (res []*MetricDesc) {
	res = make([]*MetricDesc, len(q.MetricNames))
	for i, metricName := range q.MetricNames {
		column := q.Columns[metricName]
		res[i] = column.MetricDesc(q.Name, q.LabelList())
	}
	return
}

// TimeoutDuration will turn timeout settings into time.Duration
func (q *Query) TimeoutDuration() time.Duration {
	return time.Duration(float64(time.Second) * q.Timeout)
}

// InGroups tells whether this query is in specific groups
// by matching tags with "group:" prefix. If any of the tags
// match any of the groups, return true. It's O(N^2) complexity
// but we don't really care about it since the number of tags
// and groups should both be very small.
func (q *Query) InGroups(groups []string) bool {
	for _, tag := range q.Tags {
		if strings.HasPrefix(tag, "group:") {
			for _, group := range groups {
				if tag[6:] == group {
					return true
				}
			}
		}
	}
	return false
}
