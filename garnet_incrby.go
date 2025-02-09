package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	limit2          = 500
	// numRoutines    = 10
	// updatesPerRoutine = 100
)

func updateLimiterState2(ctx context.Context, rdb *redis.Client, userID string, endpointID string, tokens int64) error {
	// Define our three window keys
	window1Key := fmt.Sprintf("ratelimit:window1:%s:%s", userID, endpointID)
	window2Key := fmt.Sprintf("ratelimit:window2:%s:%s", userID, endpointID)
	window3Key := fmt.Sprintf("ratelimit:window3:%s:%s", userID, endpointID)

	// Get current values for all windows
	pipe := rdb.Pipeline()
	window1Get := pipe.Get(ctx, window1Key)
	window2Get := pipe.Get(ctx, window2Key)
	window3Get := pipe.Get(ctx, window3Key)
	
	_, err := pipe.Exec(ctx)
	
	// Handle initial case where keys don't exist
	window1Count := int64(0)
	window2Count := int64(0)
	window3Count := int64(0)

	if err != redis.Nil {
		if w1, err := window1Get.Int64(); err == nil {
			window1Count = w1
		}
		if w2, err := window2Get.Int64(); err == nil {
			window2Count = w2
		}
		if w3, err := window3Get.Int64(); err == nil {
			window3Count = w3
		}
	}

	// Check if adding tokens would exceed limit in any window
	if window1Count+tokens > limit2 || 
	   window2Count+tokens > limit2 || 
	   window3Count+tokens > limit2 {
		return fmt.Errorf("rate limit exceeded: window1=%d, window2=%d, window3=%d, limit=%d",
			window1Count, window2Count, window3Count, limit2)
	}

	// If we're here, we can increment all counters
	pipe = rdb.Pipeline()
	pipe.IncrBy(ctx, window1Key, tokens)
	pipe.IncrBy(ctx, window2Key, tokens)
	pipe.IncrBy(ctx, window3Key, tokens)
	
	// Set expiry on keys if they're new
	pipe.Expire(ctx, window1Key, 24*time.Hour)
	pipe.Expire(ctx, window2Key, 24*time.Hour)
	pipe.Expire(ctx, window3Key, 24*time.Hour)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to increment windows: %w", err)
	}

	return nil
}

func main7() {
	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer rdb.Close()

	// Test parameters
	userID := "test_user"
	endpointID := "test_endpoint"
	
	// Initialize keys to 0
	ctx := context.Background()
	window1Key := fmt.Sprintf("ratelimit:window1:%s:%s", userID, endpointID)
	window2Key := fmt.Sprintf("ratelimit:window2:%s:%s", userID, endpointID)
	window3Key := fmt.Sprintf("ratelimit:window3:%s:%s", userID, endpointID)
	
	rdb.Set(ctx, window1Key, 0, 24*time.Hour)
	rdb.Set(ctx, window2Key, 0, 24*time.Hour)
	rdb.Set(ctx, window3Key, 0, 24*time.Hour)

	var wg sync.WaitGroup
	latencyChan := make(chan time.Duration, numRoutines*updatesPerRoutine)
	
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
				w1, _ := rdb.Get(ctx, window1Key).Int64()
				w2, _ := rdb.Get(ctx, window2Key).Int64()
				w3, _ := rdb.Get(ctx, window3Key).Int64()
				fmt.Printf("\rCurrent counts - Window1: %d/%d, Window2: %d/%d, Window3: %d/%d", 
					w1, limit2, w2, limit2, w3, limit2)
			case <-stopPrinting:
				return
			}
		}
	}()

	// Launch goroutines
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			
			for j := 0; j < updatesPerRoutine; j++ {
				start := time.Now()
				
				err := updateLimiterState2(ctx, rdb, userID, endpointID, 1)
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
	w1, _ := rdb.Get(ctx, window1Key).Int64()
	w2, _ := rdb.Get(ctx, window2Key).Int64()
	w3, _ := rdb.Get(ctx, window3Key).Int64()
	fmt.Printf("\n\nFinal State:\n")
	fmt.Printf("Window 1: %d/%d\n", w1, limit2)
	fmt.Printf("Window 2: %d/%d\n", w2, limit2)
	fmt.Printf("Window 3: %d/%d\n", w3, limit2)

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
