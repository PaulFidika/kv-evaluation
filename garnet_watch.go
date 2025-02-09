package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

 type SlidingWindow struct {
	Count     int64     `json:"count"`
	Limit     int64     `json:"limit"`
	StartTime time.Time `json:"start_time"`
 }

 type FixedWindow struct {
	Count     int64     `json:"count"`
	Limit     int64     `json:"limit"`
	StartTime time.Time `json:"start_time"`
 }

 type LimiterState struct {
	SlidingWindows []SlidingWindow `json:"sliding_windows"`
	FixedWindow    []FixedWindow   `json:"fixed_window"`
 }

func UpdateLimiterState3(rdb *redis.Client, userID string, endpointID string, tokens int64) error {
    key := fmt.Sprintf("ratelimit:%s:%s", userID, endpointID)
    ctx := context.Background()
    
    // Keep retrying until we succeed
    for {
        err := rdb.Watch(ctx, func(tx *redis.Tx) error {
            // Get the current state
            val, err := tx.Get(ctx, key).Bytes()
            
            var state LimiterState
            if err == redis.Nil {
                // Key doesn't exist, create default state
                state = LimiterState{
                    SlidingWindows: []SlidingWindow{},
                    FixedWindow:    []FixedWindow{},
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
                if state.SlidingWindows[i].Count + tokens > state.SlidingWindows[i].Limit {
                    return redis.TxFailedErr
                }
            }
            
            for i := range state.FixedWindow {
                if state.FixedWindow[i].Count + tokens > state.FixedWindow[i].Limit {
                    return redis.TxFailedErr
                }
            }
            
            // Update counters
            for i := range state.SlidingWindows {
                state.SlidingWindows[i].Count += tokens
            }
            for i := range state.FixedWindow {
                state.FixedWindow[i].Count += tokens
            }
            
            // Serialize updated state
            serialized, err := json.Marshal(state)
            if err != nil {
                return fmt.Errorf("marshal error: %w", err)
            }
            
            // Execute commands in a transaction
            _, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
                pipe.Set(ctx, key, serialized, 24*time.Hour)
                return nil
            })
            
            return err
        }, key)

        if err == redis.TxFailedErr {
            // Transaction failed, retry
            continue
        }
        return err
    }
}

func main3() {
    // Create Redis client
    rdb := redis.NewClient(&redis.Options{
        Addr: "0.0.0.0:6379",
        DB:   0,
    })
    defer rdb.Close()

    // Test key components
    userID := "test_user"
    endpointID := "test_endpoint"
    key := fmt.Sprintf("ratelimit:%s:%s", userID, endpointID)

    // Initialize state with some limits
    initialState := LimiterState{
        SlidingWindows: []SlidingWindow{
            {Count: 0, Limit: 1000, StartTime: time.Now()},
        },
        FixedWindow: []FixedWindow{
            {Count: 0, Limit: 1000, StartTime: time.Now()},
        },
    }

    // Initialize the key with initial state
    serialized, _ := json.Marshal(initialState)
    rdb.Set(context.Background(), key, serialized, 24*time.Hour)

    // Number of concurrent routines and updates per routine
    numRoutines := 10  // Yes, running 10 concurrent goroutines
    updatesPerRoutine := 100

    // Channel to collect latency measurements
    latencyChan := make(chan time.Duration, numRoutines*updatesPerRoutine)
    var wg sync.WaitGroup

    // Start a goroutine to periodically print counter values
    stopPrinting := make(chan bool)
    go func() {
        ticker := time.NewTicker(200 * time.Millisecond)
        defer ticker.Stop()
        
        for {
            select {
            case <-ticker.C:
                // Fetch and print current state
                val, err := rdb.Get(context.Background(), key).Bytes()
                if err != nil {
                    fmt.Printf("Error fetching state: %v\n", err)
                    continue
                }
                
                var state LimiterState
                if err := json.Unmarshal(val, &state); err != nil {
                    fmt.Printf("Error unmarshaling state: %v\n", err)
                    continue
                }
                
                fmt.Printf("\nCurrent Counters:\n")
                for i, sw := range state.SlidingWindows {
                    fmt.Printf("Sliding Window %d: %d/%d\n", i, sw.Count, sw.Limit)
                }
                for i, fw := range state.FixedWindow {
                    fmt.Printf("Fixed Window %d: %d/%d\n", i, fw.Count, fw.Limit)
                }
                
            case <-stopPrinting:
                return
            }
        }
    }()

    // Start time for overall execution
    startTime := time.Now()
    fmt.Printf("Starting %d goroutines with %d updates each (%d total updates)\n", 
        numRoutines, updatesPerRoutine, numRoutines*updatesPerRoutine)

    // Launch concurrent routines
    for i := 0; i < numRoutines; i++ {
        wg.Add(1)
        go func(routineID int) {
            defer wg.Done()
            
            for j := 0; j < updatesPerRoutine; j++ {
                updateStart := time.Now()
                err := UpdateLimiterState3(rdb, userID, endpointID, 1)
                latency := time.Since(updateStart)
                latencyChan <- latency

                if err != nil {
                    fmt.Printf("Error in routine %d, update %d: %v\n", routineID, j, err)
                    continue
                }
                // if !success {
                //     fmt.Printf("Rate limit exceeded in routine %d, update %d\n", routineID, j)
                //     continue
                // }
            }
        }(i)
    }

    // Wait for all routines to complete
    wg.Wait()
    close(latencyChan)
    close(stopPrinting)  // Stop the counter printing goroutine

	// Fetch and print final state
    val, err := rdb.Get(context.Background(), key).Bytes()
    if err != nil {
        fmt.Printf("Error fetching final state: %v\n", err)
    } else {
        var finalState LimiterState
        if err := json.Unmarshal(val, &finalState); err != nil {
            fmt.Printf("Error unmarshaling final state: %v\n", err)
        } else {
            fmt.Printf("\nFinal State:\n")
            for i, sw := range finalState.SlidingWindows {
                fmt.Printf("Sliding Window %d: %d/%d\n", i, sw.Count, sw.Limit)
            }
            for i, fw := range finalState.FixedWindow {
                fmt.Printf("Fixed Window %d: %d/%d\n", i, fw.Count, fw.Limit)
            }
        }
    }

    // Calculate statistics
    var totalLatency time.Duration
    var maxLatency time.Duration
    var minLatency = time.Hour // Start with a large value
    var count int
    var latencies []time.Duration

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
