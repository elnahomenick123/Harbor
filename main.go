package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

// TaskState represents the state of a replication task
type TaskState string

const (
	StatePending TaskState = "Pending"
	StateRunning TaskState = "Running"
	StateSuccess TaskState = "Success"
	StateFailed  TaskState = "Failed"
)

// Task represents a replication task in the database
type Task struct {
	ID    string
	State TaskState
	Error string
}

// Database simulates the database storing task states
type Database struct {
	mu    sync.Mutex
	tasks map[string]*Task
}

func NewDatabase() *Database {
	return &Database{
		tasks: make(map[string]*Task),
	}
}

func (db *Database) UpdateState(id string, state TaskState, err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if t, exists := db.tasks[id]; exists {
		t.State = state
		if err != nil {
			t.Error = err.Error()
		} else {
			t.Error = ""
		}
		log.Printf("Task %s updated to %s (Error: %v)", id, state, err)
	}
}

func (db *Database) GetTask(id string) *Task {
	db.mu.Lock()
	defer db.mu.Unlock()
	if t, exists := db.tasks[id]; exists {
		return &Task{ID: t.ID, State: t.State, Error: t.Error}
	}
	return nil
}

// JobService manages worker slots and executes replication tasks
type JobService struct {
	db          *Database
	workerSlots chan struct{} // Semaphore representing worker slots
	httpClient  *http.Client
}

func NewJobService(db *Database, maxWorkers int, clientTimeout time.Duration) *JobService {
	// Configure HTTP client with connection, handshake, and read/write timeouts
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: clientTimeout}).DialContext,
		TLSHandshakeTimeout:   clientTimeout,
		ResponseHeaderTimeout: clientTimeout,
		IdleConnTimeout:       clientTimeout,
	}

	return &JobService{
		db:          db,
		workerSlots: make(chan struct{}, maxWorkers),
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   clientTimeout,
		},
	}
}

// ExecuteTask runs a replication task with context propagation and timeout handling
func (js *JobService) ExecuteTask(ctx context.Context, taskID string, targetURL string) {
	// Acquire worker slot
	select {
	case js.workerSlots <- struct{}{}:
		log.Printf("Worker slot acquired for task %s", taskID)
	case <-ctx.Done():
		js.db.UpdateState(taskID, StateFailed, ctx.Err())
		return
	}

	// Ensure worker slot is freed immediately upon completion or timeout
	defer func() {
		<-js.workerSlots
		log.Printf("Worker slot freed for task %s", taskID)
	}()

	// Update state to Running
	js.db.UpdateState(taskID, StateRunning, nil)

	// Perform replication operation (HTTP request to registry)
	err := js.replicate(ctx, targetURL)
	if err != nil {
		js.db.UpdateState(taskID, StateFailed, err)
		return
	}

	js.db.UpdateState(taskID, StateSuccess, nil)
}

func (js *JobService) replicate(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := js.httpClient.Do(req)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("registry connection timeout: %w", err)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("context deadline exceeded: %w", err)
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func main() {
	// 1. Start a mock registry server that delays response to trigger timeout
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second) // Delay longer than client timeout
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	db := NewDatabase()
	taskID := "task-123"
	db.tasks[taskID] = &Task{ID: taskID, State: StatePending}

	// Create JobService with 1 second timeout
	js := NewJobService(db, 1, 1*time.Second)

	log.Printf("Starting replication task %s...", taskID)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	js.ExecuteTask(ctx, taskID, server.URL)

	// Verify task state in DB
	task := db.GetTask(taskID)
	if task.State == StateFailed {
		fmt.Printf("SUCCESS: Task transitioned to Failed state. Error: %s\n", task.Error)
	} else {
		fmt.Printf("FAILURE: Task state is %s, expected Failed\n", task.State)
	}

	// Verify worker slot is freed (we should be able to acquire it immediately)
	select {
	case js.workerSlots <- struct{}{}:
		fmt.Println("SUCCESS: Worker slot was successfully freed.")
		<-js.workerSlots
	default:
		fmt.Println("FAILURE: Worker slot was not freed.")
	}
}
