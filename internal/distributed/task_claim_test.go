package distributed

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/j3ssie/osmedeus/v5/internal/config"
)

// redisTestClient returns a client against the test Redis instance, skipping the
// test when one is not reachable. Set OSM_TEST_REDIS_PORT to point elsewhere;
// `make test-distributed` brings one up on 6399.
func redisTestClient(t *testing.T) (*Client, context.Context) {
	t.Helper()

	port := 6399
	if v := os.Getenv("OSM_TEST_REDIS_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("invalid OSM_TEST_REDIS_PORT %q: %v", v, err)
		}
		port = p
	}

	c, err := NewClient(&config.RedisConfig{Host: "127.0.0.1", Port: port})
	if err != nil {
		t.Skipf("redis not available: %v", err)
	}

	ctx := context.Background()
	if err := c.Ping(ctx); err != nil {
		c.Close()
		t.Skipf("redis not reachable on port %d: %v", port, err)
	}

	// Isolate each test; these keys are only used by the test instance.
	if err := c.client.Do(ctx, c.client.B().Flushdb().Build()).Error(); err != nil {
		c.Close()
		t.Fatalf("failed to flush test redis: %v", err)
	}
	t.Cleanup(c.Close)
	return c, ctx
}

// Regression test for #323: a task must survive its consumer being canceled at
// the instant the task arrives. Under the previous BRPOP claim, Redis handed the
// element over and the canceled caller dropped it, losing the task permanently.
func TestClaimTask_CanceledConsumerDoesNotLoseTask(t *testing.T) {
	c, ctx := redisTestClient(t)

	popCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = c.ClaimTask(popCtx, "w1", 5*time.Second)
	}()

	time.Sleep(300 * time.Millisecond)
	if err := c.PushTask(ctx, &Task{ID: "race", WorkflowName: "wf", Target: "racy"}); err != nil {
		t.Fatalf("push: %v", err)
	}
	cancel()
	<-done
	time.Sleep(200 * time.Millisecond)

	pending, _ := c.GetQueueLength(ctx, KeyTasksPending)
	inflight, _ := c.GetQueueLength(ctx, KeyTasksProcessingForWorker("w1"))
	if pending+inflight != 1 {
		t.Fatalf("task lost: pending=%d inflight=%d", pending, inflight)
	}

	if _, err := c.RecoverProcessingTasks(ctx, "w1"); err != nil {
		t.Fatalf("recover: %v", err)
	}

	task, _, err := c.ClaimTask(ctx, "w2", 2*time.Second)
	if err != nil {
		t.Fatalf("re-claim: %v", err)
	}
	if task == nil {
		t.Fatal("task not retrievable after recovery")
	}
	if task.Target != "racy" {
		t.Errorf("wrong task recovered: %q", task.Target)
	}
}

// Claiming and acknowledging drains the in-flight list and preserves FIFO order.
func TestClaimTask_AckClearsInflightAndKeepsFIFO(t *testing.T) {
	c, ctx := redisTestClient(t)

	const n = 5
	for i := 0; i < n; i++ {
		err := c.PushTask(ctx, &Task{
			ID:           fmt.Sprintf("t%d", i),
			WorkflowName: "wf",
			Target:       fmt.Sprintf("target%d", i),
		})
		if err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}

	for i := 0; i < n; i++ {
		task, payload, err := c.ClaimTask(ctx, "w1", 2*time.Second)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		if task == nil {
			t.Fatalf("claim %d returned no task", i)
		}
		if want := fmt.Sprintf("target%d", i); task.Target != want {
			t.Errorf("out of order at %d: got %q want %q", i, task.Target, want)
		}
		if err := c.AckTask(ctx, "w1", payload); err != nil {
			t.Fatalf("ack %d: %v", i, err)
		}
	}

	inflight, _ := c.GetQueueLength(ctx, KeyTasksProcessingForWorker("w1"))
	if inflight != 0 {
		t.Errorf("in-flight list not drained: %d", inflight)
	}
}

// A worker that dies after claiming but before marking the task running leaves a
// processing list the master must be able to find and drain.
func TestClaimTask_OrphanedListIsDiscoverable(t *testing.T) {
	c, ctx := redisTestClient(t)

	if err := c.PushTask(ctx, &Task{ID: "orphan", WorkflowName: "wf", Target: "gone"}); err != nil {
		t.Fatalf("push: %v", err)
	}
	if _, _, err := c.ClaimTask(ctx, "dead-worker", 2*time.Second); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Worker dies here: no ack, no SetTaskRunning.

	ids, err := c.ListProcessingWorkerIDs(ctx)
	if err != nil {
		t.Fatalf("list processing: %v", err)
	}

	var found bool
	for _, id := range ids {
		if id == "dead-worker" {
			found = true
		}
	}
	if !found {
		t.Fatalf("orphaned processing list not discoverable, got %v", ids)
	}

	if _, err := c.RecoverProcessingTasks(ctx, "dead-worker"); err != nil {
		t.Fatalf("recover: %v", err)
	}
	pending, _ := c.GetQueueLength(ctx, KeyTasksPending)
	if pending != 1 {
		t.Fatalf("task not returned to pending: %d", pending)
	}
}

// RequeueTask is the path taken when a worker cannot mark a claimed task running.
func TestRequeueTask_ReturnsTaskToPending(t *testing.T) {
	c, ctx := redisTestClient(t)

	if err := c.PushTask(ctx, &Task{ID: "rq", WorkflowName: "wf", Target: "requeued"}); err != nil {
		t.Fatalf("push: %v", err)
	}
	_, payload, err := c.ClaimTask(ctx, "w1", 2*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := c.RequeueTask(ctx, "w1", payload); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	pending, _ := c.GetQueueLength(ctx, KeyTasksPending)
	inflight, _ := c.GetQueueLength(ctx, KeyTasksProcessingForWorker("w1"))
	if pending != 1 || inflight != 0 {
		t.Fatalf("requeue left wrong state: pending=%d inflight=%d", pending, inflight)
	}
}

// Recovery on an untouched worker is a no-op rather than an error.
func TestRecoverProcessingTasks_EmptyIsNoop(t *testing.T) {
	c, ctx := redisTestClient(t)

	n, err := c.RecoverProcessingTasks(ctx, "never-seen")
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 recovered, got %d", n)
	}
}
