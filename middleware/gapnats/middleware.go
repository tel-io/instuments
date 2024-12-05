package gapnats

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	basicMessageHeaderSentTime    = "time.now"
	basicMessageHeaderServiceName = "service.name"
)

type MessageHandler func(ctx context.Context, msg *nats.Msg)

func (m MessageHandler) NatsNativeHandler(ctx context.Context) nats.MsgHandler {
	return func(msg *nats.Msg) {
		m(ctx, msg)
	}
}

type Conn interface {
	PublishMsg(ctx context.Context, msg *nats.Msg) error
	RequestMsgWithContext(ctx context.Context, msg *nats.Msg) (*nats.Msg, error)
	QueueSubscribe(ctx context.Context, subj, queue string, cb MessageHandler) (*nats.Subscription, error)
	PrintExeption(ctx context.Context, err error)
}

type ConnAdapter struct {
	serviceName string
	conn        *nats.Conn
}

func NewConnAdapter(conn *nats.Conn, serviceName string) *ConnAdapter {
	return &ConnAdapter{conn: conn, serviceName: serviceName}
}

func (c *ConnAdapter) PublishMsg(_ context.Context, msg *nats.Msg) error {
	return c.conn.PublishMsg(c.setBasicHeader(msg))
}

func (c *ConnAdapter) RequestMsgWithContext(ctx context.Context, msg *nats.Msg) (*nats.Msg, error) {
	return c.conn.RequestMsgWithContext(ctx, c.setBasicHeader(msg))
}

func (c *ConnAdapter) QueueSubscribe(
	ctx context.Context,
	subj, queue string,
	handler MessageHandler,
) (*nats.Subscription, error) {
	sub, err := c.conn.QueueSubscribe(subj, queue, handler.NatsNativeHandler(ctx))
	if err != nil {
		return nil, err
	}

	go func() {
		select {
		case <-ctx.Done():
			sub.Unsubscribe()
		}
	}()

	return sub, nil
}

func (c *ConnAdapter) PrintExeption(context.Context, error) {
	return
}

func (c *ConnAdapter) setBasicHeader(msg *nats.Msg) *nats.Msg {
	msg.Header.Add(basicMessageHeaderServiceName, c.serviceName)
	msg.Header.Add(basicMessageHeaderSentTime, time.Now().Format(time.RFC3339Nano))

	return msg
}

func CreateMiddlewares(
	serviceName string,
	conn *nats.Conn,
	metrics Metrics,
	tracer trace.Tracer,
	readers SubjectReaders,
	logger *zap.Logger,
) Conn {
	var outConn Conn = NewConnAdapter(conn, serviceName)
	outConn = NewConnWithTracer(outConn, tracer)
	outConn = NewConnWithMetrics(serviceName, outConn, metrics)
	outConn = NewConnWithLogger(outConn, readers, logger)

	return outConn
}
