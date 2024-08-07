package config

type RedisConfig struct {
	// Maximum number of idle connections in the pool.
	// Default: 10
	MaxIdle int `env:"MAX_IDLE"`

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	// Default: 100
	MaxActive int `env:"MAX_ACTIVE"`

	// Close connections after remaining idle for this duration in seconds. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	// Default: 300
	IdleTimeout int `env:"IDLE_TIMEOUT"`

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	// Default: true
	Wait bool `env:"WAIT"`

	// ReadTimeout specifies the timeout in seconds for reading a single command reply.
	// Default: 15
	ReadTimeout int `env:"READ_TIMEOUT"`

	// WriteTimeout specifies the timeout in seconds for writing a single command.
	// Default: 15
	WriteTimeout int `env:"WRITE_TIMEOUT"`

	// ConnectTimeout specifies the timeout in seconds for connecting to the Redis server when
	// no DialNetDial option is specified.
	// Default: 15
	ConnectTimeout int `env:"CONNECT_TIMEOUT"`

	// NormalTasksPollPeriod specifies the period in milliseconds when polling redis for normal tasks
	// Default: 1000
	NormalTasksPollPeriod int `env:"NORMAL_TASKS_POLL_PERIOD"`

	// DelayedTasksPollPeriod specifies the period in milliseconds when polling redis for delayed tasks
	// Default: 20
	DelayedTasksPollPeriod int    `env:"DELAYED_TASKS_POLL_PERIOD"`
	DelayedTasksKey        string `env:"DELAYED_TASKS_KEY"`

	// ClientName specifies the redis client name to be set when connecting to the Redis server
	ClientName string `env:"CLIENT_NAME"`

	// MasterName specifies a redis master name in order to configure a sentinel-backed redis FailoverClient
	MasterName string `env:"MASTER_NAME"`
	// ClusterMode specifies machinery should use redis cluster client explicitly
	ClusterMode bool `env:"CLUSTER_MODE"`
}
