package gapnats

import (
	"context"
	"sync"
	"testing"

	"git.time2go.tech/gap/dmdocker"
	"git.time2go.tech/gap/dmdocker/natstest"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

func TestMiddlewareMetrics(t *testing.T) {
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

	metricsTest1 := newMetricsTest()
	connWitMetrics := NewConnWithMetrics(test, NewConnAdapter(conn, test), metricsTest1)

	_, err = connWitMetrics.QueueSubscribe(context.Background(), test, test, func(ctx context.Context, msg *nats.Msg) {
		require.EqualValues(t, test, msg.Subject)
		require.EqualValues(t, testMsgData, msg.Data)

		answer := nats.NewMsg(msg.Reply)
		answer.Data = testMsgData
		errI := connWitMetrics.PublishMsg(ctx, answer)
		require.Nil(t, errI)
	})
	require.Nil(t, err)

	reqMsg := nats.NewMsg(test)
	reqMsg.Data = testMsgData
	res, err := connWitMetrics.RequestMsgWithContext(context.Background(), reqMsg)
	require.Nil(t, err)
	require.EqualValues(t, testMsgData, res.Data)

	require.Len(t, metricsTest1.data, 5)

}

type metricsTest struct {
	locker *sync.Mutex
	data   map[string]struct {
		Value interface{}
		Attrs []attribute.KeyValue
	}
}

func (m *metricsTest) AddRequestDuration(ctx context.Context, dur float64, attrs ...attribute.KeyValue) {
	m.locker.Lock()
	defer m.locker.Unlock()

	m.data[PublisherRequestLatency] = struct {
		Value interface{}
		Attrs []attribute.KeyValue
	}{Value: dur, Attrs: attrs}
}

func newMetricsTest() *metricsTest {
	return &metricsTest{
		locker: &sync.Mutex{},
		data: make(map[string]struct {
			Value interface{}
			Attrs []attribute.KeyValue
		}),
	}
}

func (m *metricsTest) IncRequestCount(ctx context.Context, attrs ...attribute.KeyValue) {
	m.locker.Lock()
	defer m.locker.Unlock()

	m.data[MessageCount] = struct {
		Value interface{}
		Attrs []attribute.KeyValue
	}{Value: 1, Attrs: attrs}
}

func (m *metricsTest) AddRequestContentLength(ctx context.Context, len int64, attrs ...attribute.KeyValue) {
	m.locker.Lock()
	defer m.locker.Unlock()

	m.data[ConsumerInMessageContentLength] = struct {
		Value interface{}
		Attrs []attribute.KeyValue
	}{Value: len, Attrs: attrs}
}

func (m *metricsTest) AddResponseContentLength(ctx context.Context, len int64, attrs ...attribute.KeyValue) {
	m.locker.Lock()
	defer m.locker.Unlock()

	m.data[ConsumerOutMessageContentLength] = struct {
		Value interface{}
		Attrs []attribute.KeyValue
	}{Value: len, Attrs: attrs}
}

func (m *metricsTest) AddHandleDuration(ctx context.Context, dur float64, attrs ...attribute.KeyValue) {
	m.locker.Lock()
	defer m.locker.Unlock()

	m.data[ConsumerHandleLatency] = struct {
		Value interface{}
		Attrs []attribute.KeyValue
	}{Value: dur, Attrs: attrs}
}
