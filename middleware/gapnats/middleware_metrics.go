package gapnats

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument/syncfloat64"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
)

const (
	MessageCount                    = "nats.consumer.message_count"              // Incoming request count total
	ConsumerInMessageContentLength  = "nats.consumer.in.message_content_length"  // Incoming request bytes total
	ConsumerOutMessageContentLength = "nats.consumer.out.message_content_length" // Incoming response bytes total
	ConsumerHandleLatency           = "nats.consumer.handle.duration"            // Incoming end to end duration, microseconds
	PublisherRequestLatency         = "nats.publisher.request.duration"
)

type Metrics interface {
	IncRequestCount(ctx context.Context, attrs ...attribute.KeyValue)
	AddRequestContentLength(ctx context.Context, len int64, attrs ...attribute.KeyValue)
	AddResponseContentLength(ctx context.Context, len int64, attrs ...attribute.KeyValue)
	AddHandleDuration(ctx context.Context, dur float64, attrs ...attribute.KeyValue)
	AddRequestDuration(ctx context.Context, dur float64, attrs ...attribute.KeyValue)
}

// ConnWithMetrics  Middleware Logger
type ConnWithMetrics struct {
	serviceName string
	inner       Conn
	metrics     Metrics
}

func NewConnWithMetrics(serviceName string, inner Conn, metrics Metrics) *ConnWithMetrics {
	return &ConnWithMetrics{serviceName: serviceName, inner: inner, metrics: metrics}
}

func (c *ConnWithMetrics) PublishMsg(ctx context.Context, msg *nats.Msg) error {
	if err := c.inner.PublishMsg(ctx, msg); err != nil {
		return err
	}

	labeler := new(Labeler)
	labeler.Add(attribute.String("nats.message.subject", msg.Subject))
	attributes := labeler.Get()

	c.metrics.AddResponseContentLength(ctx, int64(len(msg.Data)), attributes...)

	return nil
}

func (c *ConnWithMetrics) RequestMsgWithContext(ctx context.Context, msg *nats.Msg) (*nats.Msg, error) {
	requestStartTime := time.Now().UTC()

	out, err := c.inner.RequestMsgWithContext(ctx, msg)
	if err != nil {
		return nil, err
	}

	labeler := new(Labeler)
	labeler.Add(attribute.String("nats.message.subject", msg.Subject))
	attributes := labeler.Get()

	elapsedTime := float64(time.Since(requestStartTime)) / float64(time.Millisecond)
	c.metrics.AddRequestDuration(ctx, elapsedTime, attributes...)

	return out, nil
}

func (c *ConnWithMetrics) QueueSubscribe(
	ctx context.Context,
	subj, queue string,
	handler MessageHandler,
) (*nats.Subscription, error) {
	var inner MessageHandler = func(ctx context.Context, msg *nats.Msg) {
		requestStartTime := time.Now().UTC()

		handler(ctx, msg)

		labeler := new(Labeler)
		labeler.Add(attribute.String("nats.message.subject", msg.Subject))
		if msg.Sub != nil {
			labeler.Add(attribute.String("nats.message.queue", msg.Sub.Queue))
		}

		attributes := labeler.Get()

		elapsedTime := float64(time.Since(requestStartTime)) / float64(time.Millisecond)
		c.metrics.AddHandleDuration(ctx, elapsedTime, attributes...)
		c.metrics.IncRequestCount(ctx, attributes...)
		c.metrics.AddRequestContentLength(ctx, int64(len(msg.Data)), attributes...)
	}

	sub, err := c.inner.QueueSubscribe(ctx, subj, queue, inner)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

func (c *ConnWithMetrics) PrintExeption(ctx context.Context, err error) {
	c.inner.PrintExeption(ctx, err)
}

func RegisterBasicMetrics(meter metric.Meter) (*BasicMetrics, error) {
	rc, err := meter.SyncInt64().Counter(MessageCount)
	if err != nil {
		return nil, fmt.Errorf("meter, syncInt64, counter: %w", err)
	}

	reqCl, err := meter.SyncInt64().Counter(ConsumerInMessageContentLength)
	if err != nil {
		return nil, fmt.Errorf("meter, syncInt64, counter: %w", err)
	}

	resCl, err := meter.SyncInt64().Counter(ConsumerOutMessageContentLength)
	if err != nil {
		return nil, fmt.Errorf("meter, syncInt64, counter: %w", err)
	}

	sl, err := meter.SyncFloat64().Histogram(ConsumerHandleLatency)
	if err != nil {
		return nil, fmt.Errorf("meter, syncFloat64, histogram: %w", err)
	}

	slR, err := meter.SyncFloat64().Histogram(PublisherRequestLatency)
	if err != nil {
		return nil, fmt.Errorf("meter, syncFloat64, histogram: %w", err)
	}

	return &BasicMetrics{
		requestCount:          rc,
		requestContentLength:  reqCl,
		responseContentLength: resCl,
		handleDuration:        sl,
		requestDuration:       slR,
	}, nil
}

type BasicMetrics struct {
	requestCount          syncint64.Counter
	requestContentLength  syncint64.Counter
	responseContentLength syncint64.Counter
	handleDuration        syncfloat64.Histogram
	requestDuration       syncfloat64.Histogram
}

func (m *BasicMetrics) IncRequestCount(ctx context.Context, attrs ...attribute.KeyValue) {
	m.requestCount.Add(ctx, 1, attrs...)
}

func (m *BasicMetrics) AddRequestContentLength(ctx context.Context, len int64, attrs ...attribute.KeyValue) {
	m.requestContentLength.Add(ctx, len, attrs...)
}

func (m *BasicMetrics) AddResponseContentLength(ctx context.Context, len int64, attrs ...attribute.KeyValue) {
	m.responseContentLength.Add(ctx, len, attrs...)
}

func (m *BasicMetrics) AddHandleDuration(ctx context.Context, dur float64, attrs ...attribute.KeyValue) {
	m.handleDuration.Record(ctx, dur, attrs...)
}

func (m *BasicMetrics) AddRequestDuration(ctx context.Context, dur float64, attrs ...attribute.KeyValue) {
	m.requestDuration.Record(ctx, dur, attrs...)
}

// Labeler is used to allow instrumented HTTP handlers to add custom attributes to
// the metrics recorded by the net/http instrumentation.
type Labeler struct {
	mu         sync.Mutex
	attributes []attribute.KeyValue
}

// Add attributes to a Labeler.
func (l *Labeler) Add(ls ...attribute.KeyValue) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.attributes = append(l.attributes, ls...)
}

// Get returns a copy of the attributes added to the Labeler.
func (l *Labeler) Get() []attribute.KeyValue {
	l.mu.Lock()
	defer l.mu.Unlock()
	ret := make([]attribute.KeyValue, len(l.attributes))
	copy(ret, l.attributes)
	return ret
}
