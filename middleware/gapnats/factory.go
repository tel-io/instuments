package gapnats

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func CreateConnection(cfg Config, log *zap.Logger) (*nats.Conn, error) {
	conn, err := nats.Connect(
		cfg.URL,
		nats.UserInfo(cfg.Usr, cfg.Pwd),

		nats.RetryOnFailedConnect(cfg.ReconnectOnFailedActive),
		nats.Timeout(cfg.Timeout),
		nats.PingInterval(cfg.PingInterval),
		nats.MaxPingsOutstanding(cfg.MaxPingOut),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.MaxReconnects(cfg.MaxReconnect),
		nats.Name(cfg.ClientName),

		nats.ReconnectHandler(func(conn *nats.Conn) {
			log.Warn("nats reconnect: " + conn.Status().String())
		}),

		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			log.Error("nats disconnect status: " + conn.Status().String())
			if err != nil {
				log.Error("nats disconnect error: " + err.Error())
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("nats, connect: %w", err)
	}

	return conn, nil
}
