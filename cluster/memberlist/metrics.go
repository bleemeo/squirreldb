package memberlist

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint: gochecknoglobals
var (
	numberNodes = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "squirreldb",
		Subsystem: "memberlist",
		Name:      "number_nodes",
		Help:      "Number of nodes in the cluster",
	})
	bytesSent = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "squirreldb",
		Subsystem: "distributor",
		Name:      "bytes_send_total",
		Help:      "Total bytes send to another SquirrelDB",
	})
	requestsSecondsNode = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "nodes"},
	})
	requestsSecondsSend = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "send"},
	})
	requestsSecondsRecv = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "receive"},
	})
	requestsSecondsWaitReplay = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "wait_reply"},
	})
	requestsSecondsProcessReq = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "requests_seconds",
		Help:        "Total processing time in seconds",
		ConstLabels: prometheus.Labels{"operation": "process_request"},
	})
	messageBadFormat = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "received_message_total",
		Help:        "Total message received",
		ConstLabels: prometheus.Labels{"type": "bad_format"},
	})
	messageNoRequest = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "received_message_total",
		Help:        "Total message received",
		ConstLabels: prometheus.Labels{"type": "unmatched_reply"},
	})
	messageAfterShutdown = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "received_message_total",
		Help:        "Total message received",
		ConstLabels: prometheus.Labels{"type": "request_after_shutdown"},
	})
	messageReply = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "received_message_total",
		Help:        "Total message received",
		ConstLabels: prometheus.Labels{"type": "reply"},
	})
	messageReplyErr = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "received_message_total",
		Help:        "Total message received",
		ConstLabels: prometheus.Labels{"type": "reply_error"},
	})
	messageUnknownNode = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "received_message_total",
		Help:        "Total message received",
		ConstLabels: prometheus.Labels{"type": "request_from_unknown_node"},
	})
	messageBeforeReady = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "received_message_total",
		Help:        "Total message received",
		ConstLabels: prometheus.Labels{"type": "request_before_ready"},
	})
	messageRequest = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "received_message_total",
		Help:        "Total message received",
		ConstLabels: prometheus.Labels{"type": "request"},
	})
	sentRequestFailed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "send_message_total",
		Help:        "Total message sent",
		ConstLabels: prometheus.Labels{"type": "request_cluster_fail"},
	})
	sentReplyFailed = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "send_message_total",
		Help:        "Total message sent",
		ConstLabels: prometheus.Labels{"type": "reply_cluster_fail"},
	})
	sentTimeout = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "send_message_total",
		Help:        "Total message sent",
		ConstLabels: prometheus.Labels{"type": "request_timeout"},
	})
	sentRequest = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "send_message_total",
		Help:        "Total message sent",
		ConstLabels: prometheus.Labels{"type": "request"},
	})
	sentReply = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "squirreldb",
		Subsystem:   "memberlist",
		Name:        "send_message_total",
		Help:        "Total message sent",
		ConstLabels: prometheus.Labels{"type": "reply"},
	})
)
