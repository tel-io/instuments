package gapnats

import (
	"time"
)

type Config struct {
	Usr                     string        `env:"NATS_USER" envDefault:""`
	URL                     string        `env:"NATS_ADDR" envDefault:""`
	JetStreamName           string        `env:"NATS_JET_STREAM_NAME" envDefault:""`
	DurableName             string        `env:"NATS_DURABLE_NAME" envDefault:""`
	Token                   string        `env:"NATS_TOKEN" envDefault:""`
	ClientName              string        `env:"NATS_CLIENT_NAME" envDefault:""`
	JetStreamSubject        string        `env:"NATS_SUBJECT" envDefault:""`
	Pwd                     string        `env:"NATS_PASS" envDefault:""`
	QueueName               string        `env:"NATS_QUEUE_NAME" envDefault:""`
	Timeout                 time.Duration `env:"NATS_TIMEOUT" envDefault:"1s"`
	PingInterval            time.Duration `env:"NATS_PING_INTERVAL" envDefault:"1s"`
	MaxPingOut              int           `env:"NATS_MAX_PING_OUT" envDefault:"5"`
	MaxReconnect            int           `env:"NATS_MAX_RECONNECT" envDefault:"1000"`
	ReconnectWait           time.Duration `env:"NATS_RECONNECT_WAIT" envDefault:"1s"`
	FetchSize               int           `env:"NATS_FETCH_SIZE" envDefault:"100"`
	ReconnectOnFailedActive bool          `env:"RECONNECT_ON_FAILED_ACTIVE" envDefault:"true"`
}
