package gapnats

import (
	"context"
	"testing"

	"git.time2go.tech/gap/dmdocker"
	"git.time2go.tech/gap/dmdocker/natstest"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestMiddlewareTracer(t *testing.T) {
	container := natstest.DefaultContainer()
	containerMgr := dmdocker.NewManager().AddContainer(container)

	err := containerMgr.StartWithCheck(context.Background())
	require.Nil(t, err)

	natsOpts := nats.GetDefaultOptions()
	natsOpts.Url = container.DSN()

	conn, err := natsOpts.Connect()
	require.Nil(t, err)

	test := "test"
	testMsgData := []byte(`{"test":"test"}`)

	connWitLogger := NewConnWithTracer(NewConnAdapter(conn, test), trace.NewNoopTracerProvider().Tracer(test))

	_, err = connWitLogger.QueueSubscribe(context.Background(), test, test, func(ctx context.Context, msg *nats.Msg) {
		require.EqualValues(t, test, msg.Subject)

		span := trace.SpanFromContext(ctx)
		require.EqualValues(t, trace.TraceID{0x1}, span.SpanContext().TraceID())
		require.EqualValues(t, trace.TraceID{0x1}.String(), msg.Header.Get(RequestHeaderTraceID))

		answer := nats.NewMsg(msg.Reply)
		answer.Data = testMsgData
		errI := connWitLogger.PublishMsg(ctx, answer)
		require.Nil(t, errI)
	})
	require.Nil(t, err)

	reqMsg := nats.NewMsg(test)
	reqMsg.Data = testMsgData

	ctx := trace.ContextWithRemoteSpanContext(
		context.Background(),
		trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{0x1}}),
	)

	res, err := connWitLogger.RequestMsgWithContext(ctx, reqMsg)
	require.Nil(t, err)
	require.EqualValues(t, testMsgData, res.Data)

}
