package gapnats

import (
	"context"
	"testing"

	"git.time2go.tech/gap/dmdocker"
	"git.time2go.tech/gap/dmdocker/natstest"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestMiddlewareLogger(t *testing.T) {
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

	connWitLogger := NewConnWithLogger(NewConnAdapter(conn, test), nil, zap.NewExample())

	_, err = connWitLogger.QueueSubscribe(context.Background(), test, test, func(ctx context.Context, msg *nats.Msg) {
		require.EqualValues(t, test, msg.Subject)
		require.EqualValues(t, testMsgData, msg.Data)

		answer := nats.NewMsg(msg.Reply)
		answer.Data = testMsgData
		errI := connWitLogger.PublishMsg(ctx, answer)
		require.Nil(t, errI)
	})
	require.Nil(t, err)

	reqMsg := nats.NewMsg(test)
	reqMsg.Data = testMsgData
	res, err := connWitLogger.RequestMsgWithContext(context.Background(), reqMsg)
	require.Nil(t, err)
	require.EqualValues(t, testMsgData, res.Data)

}
