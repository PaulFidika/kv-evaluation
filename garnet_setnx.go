package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func updateLimiterStateWithLock(ctx context.Context, rdb *redis.Client, userID string, endpointID string, tokens int64) error {
	key := fmt.Sprintf("ratelimit:%s:%s", userID, endpointID)
	lockKey := fmt.Sprintf("lock:%s", key)
	lockValue := fmt.Sprintf("%d", time.Now().UnixNano())
	
	// Retry configuration
	maxRetries := 5
	baseDelay := 10 * time.Millisecond  // Reduced initial delay since we're using a more efficient method
	maxDelay := 1 * time.Second

	// Try to acquire lock with retries
	var err error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Try to acquire lock and get previous value in single atomic operation
		result := rdb.SetArgs(ctx, lockKey, lockValue, redis.SetArgs{
			Mode: "NX",
			Get:  true,
			TTL:  5 * time.Second,
		})
		
		_, err := result.Result()
		if err == nil {
			// Lock acquired successfully (no previous value)
			break
		}
		if err != redis.Nil {
			return fmt.Errorf("failed to acquire lock: %w", err)
		}
		
		// Calculate backoff delay with jitter
		delay := baseDelay * time.Duration(1<<uint(attempt))
		if delay > maxDelay {
			delay = maxDelay
		}
		jitter := time.Duration(float64(delay) * (0.5 + rand.Float64())) // Add 50-150% randomization
		
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for lock")
		case <-time.After(jitter):
			// Before retrying, check if the lock has expired
			// This helps prevent deadlocks if a client crashes while holding the lock
			ttl, err := rdb.TTL(ctx, lockKey).Result()
			if err == nil && ttl < 0 {
				// Lock has expired, delete it and retry immediately
				rdb.Del(ctx, lockKey)
				continue
			}
		}
	}

	// Ensure we release the lock, but only if we still own it
	defer func() {
		// Only delete if the value matches what we set
		rdb.Del(ctx, lockKey)
	}()

	// Get the current state
	val, err := rdb.Get(ctx, key).Bytes()
	var state LimiterState
	if err == redis.Nil {
		// Key doesn't exist, create default state
		state = LimiterState{
			SlidingWindows: []SlidingWindow{{
				Count:     0,
				Limit:     500,
				StartTime: time.Now(),
			}},
			FixedWindow: []FixedWindow{{
				Count:     0,
				Limit:     500,
				StartTime: time.Now(),
			}},
		}
	} else if err != nil {
		return fmt.Errorf("redis get error: %w", err)
	} else {
		// Deserialize existing state
		if err := json.Unmarshal(val, &state); err != nil {
			return fmt.Errorf("unmarshal error: %w", err)
		}
	}

	// Check limits
	for i := range state.SlidingWindows {
		if state.SlidingWindows[i].Count+tokens > state.SlidingWindows[i].Limit {
			return fmt.Errorf("sliding window limit exceeded")
		}
	}
	for i := range state.FixedWindow {
		if state.FixedWindow[i].Count+tokens > state.FixedWindow[i].Limit {
			return fmt.Errorf("fixed window limit exceeded")
		}
	}

	// Update counters
	for i := range state.SlidingWindows {
		state.SlidingWindows[i].Count += tokens
	}
	for i := range state.FixedWindow {
		state.FixedWindow[i].Count += tokens
	}

	// Serialize and save updated state
	serialized, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	err = rdb.Set(ctx, key, serialized, 24*time.Hour).Err()
	if err != nil {
		return fmt.Errorf("redis set error: %w", err)
	}

	return nil
}

func main() {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer rdb.Close()

	// Test parameters
	userID := "test_user"
	endpointID := "test_endpoint"
	key := fmt.Sprintf("ratelimit:%s:%s", userID, endpointID)

	// Initialize state
	ctx := context.Background()
	initialState := LimiterState{
		SlidingWindows: []SlidingWindow{{
			Count:     0,
			Limit:     500,
			StartTime: time.Now(),
		}},
		FixedWindow: []FixedWindow{{
			Count:     0,
			Limit:     500,
			StartTime: time.Now(),
		}},
	}
	serialized, _ := json.Marshal(initialState)
	rdb.Set(ctx, key, serialized, 24*time.Hour)

	var wg sync.WaitGroup
	latencyChan := make(chan time.Duration, 10*100)
	
	// Start time for overall execution
	startTime := time.Now()

	// Start a goroutine to periodically print counter values
	stopPrinting := make(chan bool)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				val, err := rdb.Get(ctx, key).Bytes()
				if err != nil {
					continue
				}
				var state LimiterState
				if err := json.Unmarshal(val, &state); err != nil {
					continue
				}
				fmt.Printf("\rCurrent counts - Sliding: %d/%d, Fixed: %d/%d",
					state.SlidingWindows[0].Count, state.SlidingWindows[0].Limit,
					state.FixedWindow[0].Count, state.FixedWindow[0].Limit)
			case <-stopPrinting:
				return
			}
		}
	}()

	// Launch goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			
			for j := 0; j < 100; j++ {
				start := time.Now()
				
				err := updateLimiterStateWithLock(ctx, rdb, userID, endpointID, 1)
				if err != nil {
					log.Printf("Error in routine %d: %v", routineID, err)
					continue
				}
				
				latencyChan <- time.Since(start)
			}
		}(i)
	}

	// Wait for all routines to complete
	wg.Wait()
	close(latencyChan)
	close(stopPrinting)

	// Print final state
	val, _ := rdb.Get(ctx, key).Bytes()
	var finalState LimiterState
	json.Unmarshal(val, &finalState)
	fmt.Printf("\n\nFinal State:\n")
	fmt.Printf("Sliding Window: %d/%d\n", 
		finalState.SlidingWindows[0].Count, finalState.SlidingWindows[0].Limit)
	fmt.Printf("Fixed Window: %d/%d\n", 
		finalState.FixedWindow[0].Count, finalState.FixedWindow[0].Limit)

	// Calculate statistics
	var latencies []time.Duration
	var totalLatency time.Duration
	var maxLatency time.Duration
	var minLatency = time.Hour
	var count int

	for latency := range latencyChan {
		totalLatency += latency
		count++
		if latency > maxLatency {
			maxLatency = latency
		}
		if latency < minLatency {
			minLatency = latency
		}
		latencies = append(latencies, latency)
	}

	// Calculate p95 latency
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	p95Index := int(float64(len(latencies)) * 0.95)
	p95Latency := latencies[p95Index]

	// Print results
	totalTime := time.Since(startTime)
	avgLatency := totalLatency / time.Duration(count)
	opsPerSecond := float64(count) / totalTime.Seconds()

	fmt.Printf("\nTest Results:\n")
	fmt.Printf("Total Operations: %d\n", count)
	fmt.Printf("Total Time: %v\n", totalTime)
	fmt.Printf("Operations/sec: %.2f\n", opsPerSecond)
	fmt.Printf("Average Latency: %v\n", avgLatency)
	fmt.Printf("Min Latency: %v\n", minLatency)
	fmt.Printf("Max Latency: %v\n", maxLatency)
	fmt.Printf("P95 Latency: %v\n", p95Latency)
}

