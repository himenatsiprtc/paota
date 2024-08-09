package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	redsyncredis "github.com/go-redsync/redsync/v4/redis/redigo"
	"github.com/gomodule/redigo/redis"
	"github.com/surendratiwari3/paota/common"
	"github.com/surendratiwari3/paota/config"
	"github.com/surendratiwari3/paota/internal/broker"
	"github.com/surendratiwari3/paota/internal/utils"
	"github.com/surendratiwari3/paota/internal/workergroup"
	"github.com/surendratiwari3/paota/logger"
	"github.com/surendratiwari3/paota/schema"
)

// RedisBroker represents an Redis broker
type RedisBroker struct {
	cnf *config.Config
	common.RedisConnector
	host                 string
	password             string
	db                   int
	pool                 *redis.Pool
	consumingWG          sync.WaitGroup // wait group to make sure whole consumption completes
	processingWG         sync.WaitGroup // use wait group to make sure task processing completes
	delayedWG            sync.WaitGroup
	socketPath           string
	redsync              *redsync.Redsync
	redisOnce            sync.Once
	redisDelayedTasksKey string
	retry                bool
	retryFunc            func(chan int)
	retryStopChan        chan int
	stopChan             chan int
}

const defaultRedisDelayedTasksKey = "delayed_tasks"

func NewRedisBroker(brokerType string, host, password, socketPath string, db int) (broker.Broker, error) {
	cfg := config.GetConfigProvider().GetConfig()
	b := &RedisBroker{
		cnf:           cfg,
		retry:         true,
		stopChan:      make(chan int),
		retryStopChan: make(chan int),
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
	b.consumingWG.Add(1)
	defer b.consumingWG.Done()

	concurrency := runtime.NumCPU() * 2

	if b.retryFunc == nil {
		b.retryFunc = utils.Closure()
	}

	conn := b.open()
	defer conn.Close()

	// Ping the server to make sure connection is live
	_, err := conn.Do("PING")

	if err != nil {
		b.retryFunc(b.retryStopChan)

		// Return err if retry is still true.
		// If retry is false, broker.StopConsuming() has been called and
		// therefore Redis might have been stopped. Return nil exit
		// StartConsuming()
		if b.retry {
			return err
		}
		return errors.New("the server has been stopped")
	}

	deliveries := make(chan []byte, concurrency)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	for i := 0; i < concurrency; i++ {
		pool <- struct{}{}
	}

	// A receiving goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {

		logger.ApplicationLogger.Info("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopChan:
				close(deliveries)
				return
			case <-pool:
				select {
				case <-b.stopChan:
					close(deliveries)
					return
				default:
				}

				task, _ := b.nextTask(b.cnf.TaskQueueName)
				//TODO: should this error be ignored?
				if len(task) > 0 {
					deliveries <- task
				}

				pool <- struct{}{}
			}
		}
	}()

	if err := b.consume(deliveries, concurrency); err != nil {
		return err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return nil
}

// nextTask pops next available task from the default queue
func (b *RedisBroker) nextTask(queue string) (result []byte, err error) {
	conn := b.open()
	defer conn.Close()

	pollPeriodMilliseconds := 1000 // default poll period for normal tasks
	if b.GetConfig().Redis != nil {
		configuredPollPeriod := b.GetConfig().Redis.NormalTasksPollPeriod
		if configuredPollPeriod > 0 {
			pollPeriodMilliseconds = configuredPollPeriod
		}
	}
	pollPeriod := time.Duration(pollPeriodMilliseconds) * time.Millisecond

	// Issue 548: BLPOP expects an integer timeout expresses in seconds.
	// The call will if the value is a float. Convert to integer using
	// math.Ceil():
	//   math.Ceil(0.0) --> 0 (block indefinitely)
	//   math.Ceil(0.2) --> 1 (timeout after 1 second)
	pollPeriodSeconds := math.Ceil(pollPeriod.Seconds())

	items, err := redis.ByteSlices(conn.Do("BLPOP", queue, pollPeriodSeconds))
	if err != nil {
		return []byte{}, err
	}

	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	if len(items) != 2 {
		return []byte{}, redis.ErrNil
	}

	result = items[1]

	return result, nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *RedisBroker) consume(deliveries <-chan []byte, concurrency int) error {
	errorsChan := make(chan error, concurrency*2)
	pool := make(chan struct{}, concurrency)

	// init pool for Worker tasks execution, as many slots as Worker concurrency param
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	for {
		select {
		case err := <-errorsChan:
			return err
		case d, open := <-deliveries:
			if !open {
				return nil
			}
			if concurrency > 0 {
				// get execution slot from pool (blocks until one is available)
				select {
				case <-b.stopChan:
					b.requeueMessage(d)
					continue
				case <-pool:
				}
			}

			b.processingWG.Add(1)

			// Consume the task inside a goroutine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give slot back to pool
					pool <- struct{}{}
				}
			}()
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *RedisBroker) consumeOne(delivery []byte) error {
	signature := new(schema.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return errors.New("Could not unmarshal message into a task signature")
	}

	// TODO : HIMEN SUTHAR : complete reque messaging if the ts=ask is not registered
	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	//if !b.IsTaskRegistered(signature.Name) {
	//	if signature.IgnoreWhenTaskNotRegistered {
	//		return nil
	//	}

	// logger.ApplicationLogger.Info("Task not registered with this worker. Requeuing message: %s", delivery)
	// b.requeueMessage(delivery, taskProcessor)
	//	return nil
	//}

	logger.ApplicationLogger.Debug("Received new message: %s", delivery)

	return nil
	//return taskProcessor.Process(signature)
}

func (b *RedisBroker) requeueMessage(delivery []byte) {
	conn := b.open()
	defer conn.Close()
	conn.Do("RPUSH", b.cnf.TaskQueueName, delivery)
}
