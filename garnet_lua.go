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
	limit3         = 500
	// numRoutines   = 10
	// updatesPerRoutine = 100
)

// Lua script to check and increment multiple rate limit3 windows atomically
// KEYS[1] = sliding window key
// KEYS[2] = fixed window key
// ARGV[1] = increment amount
// ARGV[2] = limit3
// ARGV[3] = TTL in seconds
var checkAndIncrementScript = redis.NewScript(`
	local sliding_val = redis.call('GET', KEYS[1])
	local fixed_val = redis.call('GET', KEYS[2])
	
	-- Convert to numbers, defaulting to 0 if key doesn't exist
	sliding_val = sliding_val and tonumber(sliding_val) or 0
	fixed_val = fixed_val and tonumber(fixed_val) or 0
	
	local increment = tonumber(ARGV[1])
	local limit3 = tonumber(ARGV[2])
	
	-- Check if incrementing would exceed limit3
	if sliding_val + increment > limit3 or fixed_val + increment > limit3 then
		return {sliding_val, fixed_val, 0} -- 0 indicates failure
	end
	
	-- Increment both counters
	sliding_val = redis.call('INCRBY', KEYS[1], increment)
	fixed_val = redis.call('INCRBY', KEYS[2], increment)
	
	-- Set/refresh TTL
	redis.call('EXPIRE', KEYS[1], ARGV[3])
	redis.call('EXPIRE', KEYS[2], ARGV[3])
	
	return {sliding_val, fixed_val, 1} -- 1 indicates success
`)

func updateLimiterState7(ctx context.Context, rdb *redis.Client, userID string, endpointID string, tokens int64) error {
	slidingKey := fmt.Sprintf("ratelimit:sliding:%s:%s", userID, endpointID)
	fixedKey := fmt.Sprintf("ratelimit:fixed:%s:%s", userID, endpointID)
	
	// Run the Lua script
	result, err := checkAndIncrementScript.Run(ctx, rdb,
		[]string{slidingKey, fixedKey},    // KEYS
		tokens, limit3, 24*60*60).          // ARGV (increment, limit3, TTL in seconds)
		Result()
	
	if err != nil {
		return fmt.Errorf("failed to run script: %w", err)
	}
	
	// Parse the result
	values, ok := result.([]interface{})
	if !ok || len(values) != 3 {
		return fmt.Errorf("unexpected script result format")
	}
	
	// Check if operation was successful (last value is 1 for success, 0 for failure)
	success, ok := values[2].(int64)
	if !ok || success != 1 {
		slidingVal, _ := values[0].(int64)
		fixedVal, _ := values[1].(int64)
		return fmt.Errorf("rate limit3 exceeded: sliding=%d, fixed=%d, limit3=%d",
			slidingVal, fixedVal, limit3)
	}
	
	return nil
}

func main8() {
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
	slidingKey := fmt.Sprintf("ratelimit:sliding:%s:%s", userID, endpointID)
	fixedKey := fmt.Sprintf("ratelimit:fixed:%s:%s", userID, endpointID)
	
	rdb.Set(ctx, slidingKey, 0, 24*time.Hour)
	rdb.Set(ctx, fixedKey, 0, 24*time.Hour)

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
				sliding, _ := rdb.Get(ctx, slidingKey).Int64()
				fixed, _ := rdb.Get(ctx, fixedKey).Int64()
				fmt.Printf("\rCurrent counts - Sliding: %d/%d, Fixed: %d/%d", 
					sliding, limit3, fixed, limit3)
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
				
				err := updateLimiterState7(ctx, rdb, userID, endpointID, 1)
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
	sliding, _ := rdb.Get(ctx, slidingKey).Int64()
	fixed, _ := rdb.Get(ctx, fixedKey).Int64()
	fmt.Printf("\n\nFinal State:\n")
	fmt.Printf("Sliding Window: %d/%d\n", sliding, limit3)
	fmt.Printf("Fixed Window: %d/%d\n", fixed, limit3)

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
