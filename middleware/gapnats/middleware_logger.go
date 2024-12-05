package gapnats

import (
	"context"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// SubjectReaders,SubjectReader utils for Middleware Logger

var stubReader = &SubjectReaderStub{}

type SubjectReaders map[string]SubjectReader

func (r SubjectReaders) GetReader(subj string) SubjectReader {
	if reader, ok := r[subj]; ok {
		return reader
	}

	return stubReader
}

type SubjectReader interface {
	Read(b []byte) (string, error)
}

type SubjectReaderStub struct{}

func (s *SubjectReaderStub) Read(b []byte) (string, error) { return string(b), nil }

// ConnWithLogger  Middleware Logger
type ConnWithLogger struct {
	inner          Conn
	subjectReaders SubjectReaders
	logger         *zap.Logger
}

func NewConnWithLogger(inner Conn, subjectReaders SubjectReaders, logger *zap.Logger) *ConnWithLogger {
	if subjectReaders == nil {
		subjectReaders = make(SubjectReaders)
	}

	return &ConnWithLogger{inner: inner, subjectReaders: subjectReaders, logger: logger}
}

func (c *ConnWithLogger) PublishMsg(ctx context.Context, msg *nats.Msg) error {
	log := c.buildLogger(ctx, msg)

	if err := c.inner.PublishMsg(ctx, msg); err != nil {
		log.Error("publish message", zap.Error(err))

		return err
	}

	log.Debug("publish message: success!")

	return nil
}

func (c *ConnWithLogger) RequestMsgWithContext(ctx context.Context, msg *nats.Msg) (*nats.Msg, error) {
	log := c.buildLogger(ctx, msg)

	out, err := c.inner.RequestMsgWithContext(ctx, msg)
	if err != nil {
		log.Error("request message with context", zap.Error(err))

		return nil, err
	}

	log.Debug("request message with context: success!")

	return out, nil
}

func (c *ConnWithLogger) QueueSubscribe(
	ctx context.Context,
	subj, queue string,
	handler MessageHandler,
) (*nats.Subscription, error) {
	var inner MessageHandler = func(ctx context.Context, msg *nats.Msg) {
		log := c.buildLogger(ctx, msg)

		log.Debug("handle started!")

		handler(ctx, msg)

		log.Debug("handle stopped!")
	}

	sub, err := c.inner.QueueSubscribe(ctx, subj, queue, inner)
	if err != nil {
		return nil, err
	}

	return sub, nil
}

func (c *ConnWithLogger) PrintExeption(ctx context.Context, err error) {
	span := trace.SpanFromContext(ctx)

	log := c.logger.With(
		zap.String("traceID", span.SpanContext().TraceID().String()),
		zap.String("spanID", span.SpanContext().SpanID().String()),
	)

	log.Error("handler sent", zap.Error(err))

	c.inner.PrintExeption(ctx, err)
}

func (c *ConnWithLogger) buildLogger(ctx context.Context, msg *nats.Msg) *zap.Logger {
	// set trace id and span id to logger
	span := trace.SpanFromContext(ctx)

	log := c.logger.With(
		zap.String("traceID", span.SpanContext().TraceID().String()),
		zap.String("spanID", span.SpanContext().SpanID().String()),
	)

	log = log.With(
		zap.String("nats.message.subject", msg.Subject),
		zap.String("nats.message.replay", msg.Reply),
		zap.Any("nats.message.header", msg.Header),
	)

	bodyAsStr, err := c.subjectReaders.GetReader(msg.Subject).Read(msg.Data)
	if err != nil {
		log.Error("body reader, read", zap.Error(err), zap.String("message.data", string(msg.Data)))
	} else {
		log = log.With(zap.String("message.data", bodyAsStr))
	}

	return log
}
