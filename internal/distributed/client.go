package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/j3ssie/osmedeus/v5/internal/config"
	"github.com/j3ssie/osmedeus/v5/internal/core"
	"github.com/redis/rueidis"
)

// Redis key prefixes
const (
	KeyPrefix           = "osm:"
	KeyTasksPending     = KeyPrefix + "tasks:pending"
	KeyTasksRunning     = KeyPrefix + "tasks:running"
	KeyTasksCompleted   = KeyPrefix + "tasks:completed"
	KeyTasksProcessing  = KeyPrefix + "tasks:processing:" // osm:tasks:processing:{worker_id}
	KeyWorkers          = KeyPrefix + "workers"
	KeyWorkersHeartbeat = KeyPrefix + "workers:heartbeat"
	KeyMasterLock       = KeyPrefix + "master:lock"

	// Event broker keys (pub/sub channels)
	KeyEventsPrefix = KeyPrefix + "events:" // osm:events:{topic}

	// Data queue keys (for worker -> master data)
	KeyDataRuns          = KeyPrefix + "data:runs"
	KeyDataSteps         = KeyPrefix + "data:steps"
	KeyDataEvents        = KeyPrefix + "data:events"
	KeyDataArtifacts     = KeyPrefix + "data:artifacts"
	KeyDataExecute       = KeyPrefix + "data:execute"
	KeyDataExecuteWorker = KeyPrefix + "data:execute:worker:" // osm:data:execute:worker:{worker_id}
)

// KeyTasksProcessingForWorker returns the per-worker in-flight task list key.
// A task lives here between being claimed off the pending queue and being
// recorded in KeyTasksRunning, so a worker that dies in that window does not
// take the task with it.
func KeyTasksProcessingForWorker(workerID string) string {
	return KeyTasksProcessing + workerID
}

// KeyDataExecuteForWorker returns the per-worker execute queue key.
func KeyDataExecuteForWorker(workerID string) string {
	return KeyDataExecuteWorker + workerID
}

// Timeouts and intervals
const (
	HeartbeatInterval     = 30 * time.Second
	HeartbeatTimeout      = 90 * time.Second // 3 missed heartbeats
	TaskPollTimeout       = 5 * time.Second
	DefaultConnectTimeout = 60 * time.Second
)

// Client wraps a rueidis client with helper methods
type Client struct {
	client rueidis.Client
	cfg    *config.RedisConfig
}

// NewClient creates a new Redis client from configuration
func NewClient(cfg *config.RedisConfig) (*Client, error) {
	if cfg.Host == "" {
		return nil, fmt.Errorf("redis host not configured")
	}

	port := cfg.Port
	if port == 0 {
		port = 6379
	}

	opts := rueidis.ClientOption{
		InitAddress:  []string{fmt.Sprintf("%s:%d", cfg.Host, port)},
		Username:     cfg.Username,
		Password:     cfg.Password,
		SelectDB:     cfg.DB,
		DisableCache: true, // Disable client-side caching for simpler behavior
	}

	client, err := rueidis.NewClient(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis client: %w", err)
	}

	return &Client{
		client: client,
		cfg:    cfg,
	}, nil
}

// NewClientFromConfig creates a client from the global config
func NewClientFromConfig(cfg *config.Config) (*Client, error) {
	return NewClient(&cfg.Redis)
}

// ParseRedisURL parses a Redis connection URL into RedisConfig
// Format: redis://[username:password@]host:port[/db]
func ParseRedisURL(redisURL string) (*config.RedisConfig, error) {
	if !strings.HasPrefix(redisURL, "redis://") {
		redisURL = "redis://" + redisURL
	}

	u, err := url.Parse(redisURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis URL: %w", err)
	}

	cfg := &config.RedisConfig{
		Host:              u.Hostname(),
		Port:              6379,
		ConnectionTimeout: 60,
	}

	if u.Port() != "" {
		port, err := strconv.Atoi(u.Port())
		if err != nil {
			return nil, fmt.Errorf("invalid redis port: %w", err)
		}
		cfg.Port = port
	}

	if u.User != nil {
		cfg.Username = u.User.Username()
		cfg.Password, _ = u.User.Password()
	}

	if u.Path != "" && u.Path != "/" {
		db, err := strconv.Atoi(strings.TrimPrefix(u.Path, "/"))
		if err == nil {
			cfg.DB = db
		}
	}

	return cfg, nil
}

// Close closes the Redis client
func (c *Client) Close() {
	c.client.Close()
}

// Ping tests the Redis connection
func (c *Client) Ping(ctx context.Context) error {
	cmd := c.client.B().Ping().Build()
	return c.client.Do(ctx, cmd).Error()
}

// Raw returns the underlying rueidis client
func (c *Client) Raw() rueidis.Client {
	return c.client
}

// PushTask pushes a task to the pending queue
func (c *Client) PushTask(ctx context.Context, task *Task) error {
	data, err := task.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	cmd := c.client.B().Lpush().Key(KeyTasksPending).Element(string(data)).Build()
	return c.client.Do(ctx, cmd).Error()
}

// PopTask pops a task from the pending queue (blocking).
//
// Deprecated: BRPOP is at-most-once -- if the caller dies, is canceled, or loses
// the connection between Redis popping the element and the caller recording it,
// the task is gone for good. Use ClaimTask, which moves the task to a per-worker
// processing list in the same atomic step. Retained for the task-queue poller in
// pkg/cli/worker_queue.go, which re-queues from the database instead.
func (c *Client) PopTask(ctx context.Context, timeout time.Duration) (*Task, error) {
	cmd := c.client.B().Brpop().Key(KeyTasksPending).Timeout(timeout.Seconds()).Build()
	result, err := c.client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil // Timeout, no task available
		}
		return nil, fmt.Errorf("failed to pop task: %w", err)
	}

	if len(result) < 2 {
		return nil, nil // No task
	}

	return UnmarshalTask([]byte(result[1]))
}

// ClaimTask atomically moves a task from the pending queue onto the calling
// worker's processing list and returns it, along with the raw payload needed to
// acknowledge it later.
//
// The move is a single BLMOVE, so the task is never held only in the worker's
// memory: if the worker dies before finishing, the task stays on its processing
// list and RecoverProcessingTasks puts it back. Requires Redis 6.2+.
func (c *Client) ClaimTask(ctx context.Context, workerID string, timeout time.Duration) (*Task, string, error) {
	// RIGHT off pending keeps FIFO order (PushTask LPUSHes onto the head).
	cmd := c.client.B().Blmove().
		Source(KeyTasksPending).
		Destination(KeyTasksProcessingForWorker(workerID)).
		Right().
		Left().
		Timeout(timeout.Seconds()).
		Build()

	payload, err := c.client.Do(ctx, cmd).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, "", nil // Timeout, no task available
		}
		return nil, "", fmt.Errorf("failed to claim task: %w", err)
	}

	task, err := UnmarshalTask([]byte(payload))
	if err != nil {
		// Payload is unusable; drop it from the processing list so it does not
		// get replayed forever on every recovery sweep.
		_ = c.AckTask(ctx, workerID, payload)
		return nil, "", fmt.Errorf("failed to unmarshal claimed task: %w", err)
	}

	return task, payload, nil
}

// AckTask removes a claimed task from a worker's processing list, marking it as
// no longer in flight. Safe to call more than once.
func (c *Client) AckTask(ctx context.Context, workerID, payload string) error {
	cmd := c.client.B().Lrem().
		Key(KeyTasksProcessingForWorker(workerID)).
		Count(1).
		Element(payload).
		Build()
	return c.client.Do(ctx, cmd).Error()
}

// RequeueTask returns a single claimed task to the pending queue. If the LREM
// succeeds but the push does not, the task stays on the processing list and is
// picked up by RecoverProcessingTasks, so no path drops it.
func (c *Client) RequeueTask(ctx context.Context, workerID, payload string) error {
	task, err := UnmarshalTask([]byte(payload))
	if err != nil {
		return fmt.Errorf("failed to unmarshal task for requeue: %w", err)
	}
	if err := c.PushTask(ctx, task); err != nil {
		return err
	}
	return c.AckTask(ctx, workerID, payload)
}

// RecoverProcessingTasks moves every task still on a worker's processing list
// back onto the pending queue and returns how many were recovered. Used when a
// worker is found dead, and by a worker itself on startup to reclaim tasks it
// lost to an earlier crash.
func (c *Client) RecoverProcessingTasks(ctx context.Context, workerID string) (int, error) {
	key := KeyTasksProcessingForWorker(workerID)
	recovered := 0

	for {
		// Onto the tail of pending, so a recovered task is taken next rather
		// than queueing behind everything submitted while the worker was down.
		cmd := c.client.B().Lmove().
			Source(key).
			Destination(KeyTasksPending).
			Right().
			Right().
			Build()

		if err := c.client.Do(ctx, cmd).Error(); err != nil {
			if rueidis.IsRedisNil(err) {
				return recovered, nil // List drained
			}
			return recovered, fmt.Errorf("failed to recover in-flight tasks: %w", err)
		}
		recovered++
	}
}

// ListProcessingWorkerIDs returns the worker IDs that currently have a
// processing list in Redis, including workers that are no longer registered.
func (c *Client) ListProcessingWorkerIDs(ctx context.Context) ([]string, error) {
	var ids []string
	cursor := uint64(0)

	for {
		cmd := c.client.B().Scan().Cursor(cursor).Match(KeyTasksProcessing + "*").Count(100).Build()
		entry, err := c.client.Do(ctx, cmd).AsScanEntry()
		if err != nil {
			return nil, fmt.Errorf("failed to scan processing lists: %w", err)
		}
		for _, key := range entry.Elements {
			ids = append(ids, strings.TrimPrefix(key, KeyTasksProcessing))
		}
		if entry.Cursor == 0 {
			return ids, nil
		}
		cursor = entry.Cursor
	}
}

// SetTaskRunning moves a task to the running hash
func (c *Client) SetTaskRunning(ctx context.Context, task *Task) error {
	data, err := task.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal task: %w", err)
	}

	cmd := c.client.B().Hset().Key(KeyTasksRunning).FieldValue().FieldValue(task.ID, string(data)).Build()
	return c.client.Do(ctx, cmd).Error()
}

// RemoveTaskRunning removes a task from the running hash
func (c *Client) RemoveTaskRunning(ctx context.Context, taskID string) error {
	cmd := c.client.B().Hdel().Key(KeyTasksRunning).Field(taskID).Build()
	return c.client.Do(ctx, cmd).Error()
}

// SetTaskResult stores a task result in the completed hash
func (c *Client) SetTaskResult(ctx context.Context, result *TaskResult) error {
	data, err := result.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	cmd := c.client.B().Hset().Key(KeyTasksCompleted).FieldValue().FieldValue(result.TaskID, string(data)).Build()
	return c.client.Do(ctx, cmd).Error()
}

// GetTaskResult retrieves a task result from the completed hash
func (c *Client) GetTaskResult(ctx context.Context, taskID string) (*TaskResult, error) {
	cmd := c.client.B().Hget().Key(KeyTasksCompleted).Field(taskID).Build()
	data, err := c.client.Do(ctx, cmd).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get task result: %w", err)
	}

	return UnmarshalTaskResult([]byte(data))
}

// GetRunningTask retrieves a running task by ID
func (c *Client) GetRunningTask(ctx context.Context, taskID string) (*Task, error) {
	cmd := c.client.B().Hget().Key(KeyTasksRunning).Field(taskID).Build()
	data, err := c.client.Do(ctx, cmd).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get running task: %w", err)
	}

	return UnmarshalTask([]byte(data))
}

// GetAllRunningTasks retrieves all running tasks
func (c *Client) GetAllRunningTasks(ctx context.Context) ([]*Task, error) {
	cmd := c.client.B().Hgetall().Key(KeyTasksRunning).Build()
	result, err := c.client.Do(ctx, cmd).AsStrMap()
	if err != nil {
		return nil, fmt.Errorf("failed to get running tasks: %w", err)
	}

	var tasks []*Task
	for _, data := range result {
		task, err := UnmarshalTask([]byte(data))
		if err != nil {
			continue
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

// RegisterWorker registers a worker in the workers hash
func (c *Client) RegisterWorker(ctx context.Context, worker *WorkerInfo) error {
	data, err := worker.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal worker: %w", err)
	}

	cmd := c.client.B().Hset().Key(KeyWorkers).FieldValue().FieldValue(worker.ID, string(data)).Build()
	return c.client.Do(ctx, cmd).Error()
}

// UpdateWorkerHeartbeat updates a worker's heartbeat timestamp
func (c *Client) UpdateWorkerHeartbeat(ctx context.Context, workerID string) error {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	cmd := c.client.B().Hset().Key(KeyWorkersHeartbeat).FieldValue().FieldValue(workerID, timestamp).Build()
	return c.client.Do(ctx, cmd).Error()
}

// GetWorkerHeartbeat gets a worker's last heartbeat timestamp
func (c *Client) GetWorkerHeartbeat(ctx context.Context, workerID string) (time.Time, error) {
	cmd := c.client.B().Hget().Key(KeyWorkersHeartbeat).Field(workerID).Build()
	data, err := c.client.Do(ctx, cmd).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return time.Time{}, nil
		}
		return time.Time{}, err
	}

	ts, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(ts, 0), nil
}

// GetAllWorkers retrieves all registered workers
func (c *Client) GetAllWorkers(ctx context.Context) ([]*WorkerInfo, error) {
	cmd := c.client.B().Hgetall().Key(KeyWorkers).Build()
	result, err := c.client.Do(ctx, cmd).AsStrMap()
	if err != nil {
		return nil, fmt.Errorf("failed to get workers: %w", err)
	}

	var workers []*WorkerInfo
	for _, data := range result {
		worker, err := UnmarshalWorkerInfo([]byte(data))
		if err != nil {
			continue
		}
		workers = append(workers, worker)
	}

	return workers, nil
}

// GetWorker retrieves a single worker by ID
func (c *Client) GetWorker(ctx context.Context, workerID string) (*WorkerInfo, error) {
	cmd := c.client.B().Hget().Key(KeyWorkers).Field(workerID).Build()
	data, err := c.client.Do(ctx, cmd).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get worker: %w", err)
	}
	return UnmarshalWorkerInfo([]byte(data))
}

// GetWorkerByAlias retrieves a worker by its alias
func (c *Client) GetWorkerByAlias(ctx context.Context, alias string) (*WorkerInfo, error) {
	workers, err := c.GetAllWorkers(ctx)
	if err != nil {
		return nil, err
	}
	for _, w := range workers {
		if w.Alias == alias {
			return w, nil
		}
	}
	return nil, nil
}

// RemoveWorker removes a worker from the registry
func (c *Client) RemoveWorker(ctx context.Context, workerID string) error {
	// Remove from both workers and heartbeat hashes
	cmd1 := c.client.B().Hdel().Key(KeyWorkers).Field(workerID).Build()
	cmd2 := c.client.B().Hdel().Key(KeyWorkersHeartbeat).Field(workerID).Build()

	if err := c.client.Do(ctx, cmd1).Error(); err != nil {
		return err
	}
	return c.client.Do(ctx, cmd2).Error()
}

// AcquireMasterLock tries to acquire the master lock
func (c *Client) AcquireMasterLock(ctx context.Context, masterID string, ttl time.Duration) (bool, error) {
	cmd := c.client.B().Set().Key(KeyMasterLock).Value(masterID).Nx().Ex(ttl).Build()
	result, err := c.client.Do(ctx, cmd).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return false, nil // Lock not acquired
		}
		return false, err
	}
	return result == "OK", nil
}

// RefreshMasterLock refreshes the master lock TTL
func (c *Client) RefreshMasterLock(ctx context.Context, masterID string, ttl time.Duration) error {
	// Only refresh if we still own the lock
	cmd := c.client.B().Get().Key(KeyMasterLock).Build()
	current, err := c.client.Do(ctx, cmd).ToString()
	if err != nil {
		return err
	}
	if current != masterID {
		return fmt.Errorf("master lock lost")
	}

	expireCmd := c.client.B().Expire().Key(KeyMasterLock).Seconds(int64(ttl.Seconds())).Build()
	return c.client.Do(ctx, expireCmd).Error()
}

// ReleaseMasterLock releases the master lock
func (c *Client) ReleaseMasterLock(ctx context.Context, masterID string) error {
	// Only release if we own the lock
	cmd := c.client.B().Get().Key(KeyMasterLock).Build()
	current, err := c.client.Do(ctx, cmd).ToString()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil // Already released
		}
		return err
	}
	if current != masterID {
		return nil // Not our lock
	}

	delCmd := c.client.B().Del().Key(KeyMasterLock).Build()
	return c.client.Do(ctx, delCmd).Error()
}

// =============================================================================
// Event Pub/Sub Methods
// =============================================================================

// PublishEvent publishes an event to a topic channel
func (c *Client) PublishEvent(ctx context.Context, topic string, event *core.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	channel := KeyEventsPrefix + topic
	cmd := c.client.B().Publish().Channel(channel).Message(string(data)).Build()
	return c.client.Do(ctx, cmd).Error()
}

// SubscribeEvents subscribes to event channels with pattern and calls handler for each event.
// This method blocks until the context is cancelled or an error occurs.
func (c *Client) SubscribeEvents(ctx context.Context, handler func(*core.Event)) error {
	pattern := KeyEventsPrefix + "*"

	// Use Receive with PSUBSCRIBE - this blocks and calls handler for each message
	err := c.client.Receive(ctx, c.client.B().Psubscribe().Pattern(pattern).Build(),
		func(msg rueidis.PubSubMessage) {
			if msg.Message != "" {
				var event core.Event
				if err := json.Unmarshal([]byte(msg.Message), &event); err == nil {
					handler(&event)
				}
			}
		})

	if err != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// =============================================================================
// Data Queue Methods (Worker -> Master)
// =============================================================================

// DataEnvelope wraps data with type information for queue processing
type DataEnvelope struct {
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	Timestamp time.Time       `json:"timestamp"`
	WorkerID  string          `json:"worker_id,omitempty"`
}

// PushData pushes data to a worker data queue
func (c *Client) PushData(ctx context.Context, key string, dataType string, data interface{}, workerID string) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	envelope := DataEnvelope{
		Type:      dataType,
		Data:      dataBytes,
		Timestamp: time.Now(),
		WorkerID:  workerID,
	}

	envBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to marshal envelope: %w", err)
	}

	cmd := c.client.B().Lpush().Key(key).Element(string(envBytes)).Build()
	return c.client.Do(ctx, cmd).Error()
}

// PopData pops data from a queue (blocking with timeout)
func (c *Client) PopData(ctx context.Context, key string, timeout time.Duration) (*DataEnvelope, error) {
	cmd := c.client.B().Brpop().Key(key).Timeout(timeout.Seconds()).Build()
	result, err := c.client.Do(ctx, cmd).AsStrSlice()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return nil, nil // Timeout, no data available
		}
		return nil, fmt.Errorf("failed to pop data: %w", err)
	}

	if len(result) < 2 {
		return nil, nil // No data
	}

	var envelope DataEnvelope
	if err := json.Unmarshal([]byte(result[1]), &envelope); err != nil {
		return nil, fmt.Errorf("failed to unmarshal envelope: %w", err)
	}

	return &envelope, nil
}

// PopDataMulti pops data from multiple queues (blocking with timeout)
// Returns the key that had data and the data envelope
func (c *Client) PopDataMulti(ctx context.Context, timeout time.Duration, keys ...string) (string, *DataEnvelope, error) {
	cmd := c.client.B().Brpop().Key(keys[0])
	for _, k := range keys[1:] {
		cmd = cmd.Key(k)
	}
	cmdBuilt := cmd.Timeout(timeout.Seconds()).Build()

	result, err := c.client.Do(ctx, cmdBuilt).AsStrSlice()
	if err != nil {
		if rueidis.IsRedisNil(err) {
			return "", nil, nil // Timeout, no data available
		}
		return "", nil, fmt.Errorf("failed to pop data: %w", err)
	}

	if len(result) < 2 {
		return "", nil, nil // No data
	}

	var envelope DataEnvelope
	if err := json.Unmarshal([]byte(result[1]), &envelope); err != nil {
		return "", nil, fmt.Errorf("failed to unmarshal envelope: %w", err)
	}

	return result[0], &envelope, nil
}

// GetQueueLength returns the length of a data queue
func (c *Client) GetQueueLength(ctx context.Context, key string) (int64, error) {
	cmd := c.client.B().Llen().Key(key).Build()
	return c.client.Do(ctx, cmd).AsInt64()
}
