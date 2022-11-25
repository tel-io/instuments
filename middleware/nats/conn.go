package nats

import (
	"context"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/tel-io/tel/v2"
)

// Core features for context
type Core struct {
	*config

	list []Middleware

	subMeter *SubscriptionStatMetric
}

// ConnContext wrapper for nats.ConnContext aks mw connection approach
//
// Features:
// Expose subscription stats via function overwrite
type ConnContext struct {
	conn *nats.Conn
	Publish

	*Core
}

func newCore(opts ...Option) *Core {
	cfg := newConfig(opts)

	sb, err := NewSubscriptionStatMetrics(opts...)
	if err != nil {
		cfg.tele.Panic("wrap connection", tel.Error(err))
	}

	return &Core{
		config:   cfg,
		subMeter: sb,
		list:     cfg.Middleware(),
	}
}

// New wraps nats Core connection with middleware functionality
func New(conn *nats.Conn, opts ...Option) *ConnContext {
	core := newCore(opts...)

	// init publish
	var pub PubMiddleware
	pub = NewCommonPublish(conn)

	pubList := core.DefaultPubMiddleware()
	for _, mw := range pubList {
		pub = mw.apply(pub)
	}

	return &ConnContext{
		conn:    conn,
		Core:    core,
		Publish: pub,
	}
}

// wrap Middleware wrap
func (c *Core) wrap(in MsgHandler) nats.MsgHandler {
	for _, cb := range c.list {
		in = cb.apply(in)
	}

	return func(msg *nats.Msg) {
		// init context for instance
		cxt := c.config.tele.Copy().Ctx()
		_ = in(cxt, msg)
	}
}

// Conn unwrap connection
func (c *ConnContext) Conn() *nats.Conn {
	return c.conn
}

// JetStream returns a JetStreamContext wrapper for consumer
func (c *ConnContext) JetStream(opts ...nats.JSOpt) (*JetStreamContext, error) {
	js, err := c.conn.JetStream(opts...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &JetStreamContext{
		js:   js,
		Core: c.Core,
	}, nil
}

// Subscribe will express interest in the given subject. The subject
// can have wildcards.
// There are two type of wildcards: * for partial, and > for full.
// A subscription on subject time.*.east would receive messages sent to time.us.east and time.eu.east.
// A subscription on subject time.us.> would receive messages sent to
// time.us.east and time.us.east.atlanta, while time.us.* would only match time.us.east
// since it can't match more than one token.
// Messages will be delivered to the associated MsgHandler.
func (c *ConnContext) Subscribe(subj string, cb MsgHandler) (*nats.Subscription, error) {
	return c.subMeter.Hook(
		c.conn.Subscribe(subj, c.wrap(cb)),
	)
}

// QueueSubscribe creates an asynchronous queue subscriber on the given subject.
// All subscribers with the same queue name will form the queue group and
// only one member of the group will be selected to receive any given
// message asynchronously.
func (c *ConnContext) QueueSubscribe(subj, queue string, cb MsgHandler) (*nats.Subscription, error) {
	return c.subMeter.Hook(
		c.conn.QueueSubscribe(subj, queue, c.wrap(cb)),
	)
}

// QueueSubscribeMW mw callback function, just legacy
// Deprecated: just backport compatibility for PostFn legacy
func (c *ConnContext) QueueSubscribeMW(subj, queue string, next PostFn) (*nats.Subscription, error) {
	return c.QueueSubscribe(subj, queue, func(ctx context.Context, msg *nats.Msg) error {
		resp, err := next(ctx, msg.Subject, msg.Data)
		if err != nil || c.config.postHook == nil {
			return nil
		}

		err = c.config.postHook(ctx, msg, resp)
		return nil
	})
}

// SubscribeMW backport compatible function for previous mw approach
// Deprecated: just backport compatibility for PostFn legacy
func (c *ConnContext) SubscribeMW(subj string, cb PostFn) (*nats.Subscription, error) {
	return c.QueueSubscribeMW(subj, "", cb)
}

// BuildWrappedHandler allow to create own mw, for bach processing for example or so on
func (c *ConnContext) BuildWrappedHandler(cb MsgHandler) nats.MsgHandler {
	return c.wrap(cb)
}