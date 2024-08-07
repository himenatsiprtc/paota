package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis/redigo"
	"github.com/gomodule/redigo/redis"
	"github.com/surendratiwari3/paota/common"
	"github.com/surendratiwari3/paota/config"
	"github.com/surendratiwari3/paota/internal/broker"
	"github.com/surendratiwari3/paota/internal/workergroup"
	"github.com/surendratiwari3/paota/schema"
)

// RedisBroker represents an Redis broker
type RedisBroker struct {
	cnf *config.Config
	common.RedisConnector
	host         string
	password     string
	db           int
	pool         *redis.Pool
	consumingWG  sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG    sync.WaitGroup
	// If set, path to a socket file overrides hostname
	socketPath           string
	redsync              *redsync.Redsync
	redisOnce            sync.Once
	redisDelayedTasksKey string
}

const defaultRedisDelayedTasksKey = "delayed_tasks"

func NewRedisBroker(brokerType string, host, password, socketPath string, db int) (broker.Broker, error) {
	cfg := config.GetConfigProvider().GetConfig()
	b := &RedisBroker{
		cnf: cfg,
	}

	b.host = host
	b.db = db
	b.password = password
	b.socketPath = socketPath

	if cfg.Redis != nil && cfg.Redis.DelayedTasksKey != "" {
		b.redisDelayedTasksKey = cfg.Redis.DelayedTasksKey
	} else {
		b.redisDelayedTasksKey = defaultRedisDelayedTasksKey
	}

	return b, nil
}

// Publish sends a task to the RedisBroker
func (b *RedisBroker) Publish(ctx context.Context, signature *schema.Signature) error {
	_, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	// Adjust routing key (this decides which queue the message will be published to)
	b.AdjustRoutingKey(signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	conn := b.open()
	defer conn.Close()

	// Check the ETA signature field, if it is set and it is in the future,
	// delay the task
	if signature.ETA != nil {
		now := time.Now().UTC()

		if signature.ETA.After(now) {
			score := signature.ETA.UnixNano()
			_, err = conn.Do("ZADD", b.redisDelayedTasksKey, score, msg)
			return err
		}
	}

	_, err = conn.Do("RPUSH", signature.RoutingKey, msg)
	return err
}

// GetConfig returns config
func (b *RedisBroker) GetConfig() *config.Config {
	return b.cnf
}

func (b *RedisBroker) AdjustRoutingKey(s *schema.Signature) {
	if s.RoutingKey != "" {
		return
	}

	s.RoutingKey = b.GetConfig().TaskQueueName
}

// open returns or creates instance of Redis connection
func (b *RedisBroker) open() redis.Conn {
	b.redisOnce.Do(func() {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db, b.GetConfig().Redis, b.GetConfig().TLSConfig)
		b.redsync = redsync.New(redsyncredis.NewPool(b.pool))
	})

	return b.pool.Get()
}

// StopConsumer stops the Redis consumer
func (b *RedisBroker) StopConsumer() {

}

// StartConsumer initializes the AMQP consumer
func (b *RedisBroker) StartConsumer(ctx context.Context, workerGroup workergroup.WorkerGroupInterface) error {
	return nil
}
