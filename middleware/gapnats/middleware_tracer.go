package gapnats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	RequestHeaderTraceID = "x-trace-id"
)

type Header interface {
	Set(key string, value string)
	Get(key string) string
}

func extractRemoteTraceID(header Header) (trace.TraceID, bool) {
	traceIdAsString := header.Get(RequestHeaderTraceID)
	if traceIdAsString == "" {
		return trace.TraceID{}, false
	}

	traceId, err := trace.TraceIDFromHex(traceIdAsString)
	if err != nil {
		return trace.TraceID{}, false
	}

	return traceId, true
}

// ConnWithTrace   Middleware Logger
type ConnWithTrace struct {
	inner  Conn
	tracer trace.Tracer
}

func NewConnWithTracer(inner Conn, tracer trace.Tracer) *ConnWithTrace {
	return &ConnWithTrace{inner: inner, tracer: tracer}
}

// PublishMsg - context must be with trace
func (c *ConnWithTrace) PublishMsg(ctx context.Context, msg *nats.Msg) error {
	span := trace.SpanFromContext(ctx)

	msg.Header.Add(RequestHeaderTraceID, span.SpanContext().TraceID().String())

	if err := c.inner.PublishMsg(ctx, msg); err != nil {
		span.RecordError(err)
		return err
	}

	span.AddEvent(
		"nats.publish.success",
		trace.WithAttributes(
			attribute.String("nats.message.subject", msg.Subject),
		),
	)

	return nil
}

// RequestMsgWithContext - context must be with trace
func (c *ConnWithTrace) RequestMsgWithContext(ctx context.Context, msg *nats.Msg) (*nats.Msg, error) {
	span := trace.SpanFromContext(ctx)

	msg.Header.Add(RequestHeaderTraceID, span.SpanContext().TraceID().String())
	out, err := c.inner.RequestMsgWithContext(ctx, msg)
	if err != nil {
		span.RecordError(err)

		return nil, err
	}

	span.AddEvent(
		"nats.request.success",
		trace.WithAttributes(
			attribute.String("nats.message.subject", msg.Subject),
		),
	)

	return out, nil
}

func (c *ConnWithTrace) QueueSubscribe(
	ctx context.Context,
	subj, queue string,
	handler MessageHandler,
) (*nats.Subscription, error) {
	var inner MessageHandler = func(ctx context.Context, msg *nats.Msg) {
		reqCtx := ctx
		rTraceID, isRemote := extractRemoteTraceID(msg.Header)
		if isRemote {
			remoteSpanCtx := trace.NewSpanContext(trace.SpanContextConfig{
				TraceID:    rTraceID,
				SpanID:     trace.SpanID{},
				TraceFlags: 0,
				TraceState: trace.TraceState{},
				Remote:     isRemote,
			})

			reqCtx = trace.ContextWithRemoteSpanContext(ctx, remoteSpanCtx)
		}

		ctxWithSpan, span := c.tracer.Start(reqCtx, makeSpanName(subj))

		span.SetAttributes(
			attribute.Bool("trace.id.remoted", isRemote),
			attribute.String("nats.subject", msg.Subject),
			attribute.String("nats.header.service_name", msg.Header.Get(basicMessageHeaderServiceName)),
			attribute.String("nats.header.sent_time", msg.Header.Get(basicMessageHeaderSentTime)),
			attribute.String("nats.header.start_time", time.Now().UTC().Format(time.RFC3339Nano)),
			attribute.Int("nats.message.content_length", len(msg.Data)),
		)

		handler(ctxWithSpan, msg)

		span.End()
	}

	sub, err := c.inner.QueueSubscribe(ctx, subj, queue, inner)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

func (c *ConnWithTrace) PrintExeption(ctx context.Context, err error) {
	c.inner.PrintExeption(ctx, err)
}

func makeSpanName(path string) string {
	return fmt.Sprintf("nats.handler.%s", path)
}
