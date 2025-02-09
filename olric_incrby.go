package main

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
)

func updateLimiterState5(ctx context.Context, dm olric.DMap, userID string, endpointID string, tokens int64) error {
    slidingKey := fmt.Sprintf("ratelimit:sliding:%s:%s", userID, endpointID)
    fixedKey := fmt.Sprintf("ratelimit:fixed:%s:%s", userID, endpointID)
    maxRetries := 5

    for i := 0; i < maxRetries; i++ {
        // First check current counts
        slidingVal, err := dm.Get(ctx, slidingKey)
        if err != nil && err != olric.ErrKeyNotFound {
            if err == olric.ErrWriteQuorum {
                time.Sleep(time.Millisecond * 10)
                continue
            }
            return fmt.Errorf("failed to get sliding window count: %w", err)
        }
        
        fixedVal, err := dm.Get(ctx, fixedKey)
        if err != nil && err != olric.ErrKeyNotFound {
            if err == olric.ErrWriteQuorum {
                time.Sleep(time.Millisecond * 10)
                continue
            }
            return fmt.Errorf("failed to get fixed window count: %w", err)
        }

        // Get current counts, defaulting to 0 if not found
        slidingCount := int64(0)
        if slidingVal != nil {
            slidingCount, _ = slidingVal.Int64()
        }
        
        fixedCount := int64(0)
        if fixedVal != nil {
            fixedCount, _ = fixedVal.Int64()
        }

        // Check if adding tokens would exceed limits
        if slidingCount + tokens > 500 || fixedCount + tokens > 500 {
            return fmt.Errorf("rate limit exceeded: sliding=%d, fixed=%d, limit=500", slidingCount, fixedCount)
        }

        // If we're here, we can increment both counters
        _, err = dm.Incr(ctx, slidingKey, int(tokens))
        if err != nil {
            if err == olric.ErrWriteQuorum {
                time.Sleep(time.Millisecond * 10)
                continue
            }
            return fmt.Errorf("failed to increment sliding window: %w", err)
        }

        _, err = dm.Incr(ctx, fixedKey, int(tokens))
        if err != nil {
            if err == olric.ErrWriteQuorum {
                time.Sleep(time.Millisecond * 10)
                continue
            }
            return fmt.Errorf("failed to increment fixed window: %w", err)
        }

        return nil
    }
    return fmt.Errorf("failed to update after %d retries", maxRetries)
}

func main5() {
    // Create Olric config
    c := config.New("local")
    
    // Create context for startup
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Create and start Olric instance
    db, err := olric.New(c)
    if err != nil {
        log.Fatalf("Failed to create Olric instance: %v", err)
    }

    // Start Olric in the main goroutine
    go func() {
        if err := db.Start(); err != nil {
            log.Fatalf("olric.Start returned an error: %v", err)
        }
    }()

    // Wait a bit for the server to be ready
    time.Sleep(2 * time.Second)

    // Create embedded client
    client := db.NewEmbeddedClient()

    // Create DMap
    dm, err := client.NewDMap("rate-limiter")
    if err != nil {
        log.Fatalf("Failed to create DMap: %v", err)
    }

    // Test parameters
    numRoutines := 10
    updatesPerRoutine := 100
    
    var wg sync.WaitGroup
    latencyChan := make(chan time.Duration, numRoutines*updatesPerRoutine)
    
    // Start time for overall execution
    startTime := time.Now()

    // Use a single shared key for all routines
    userID := "test_user"
    endpointID := "test_endpoint"
    slidingKey := fmt.Sprintf("ratelimit:sliding:%s:%s", userID, endpointID)
    fixedKey := fmt.Sprintf("ratelimit:fixed:%s:%s", userID, endpointID)

    // Initialize counters to 0
    if err := dm.Put(context.Background(), slidingKey, 0); err != nil {
        log.Fatalf("Failed to initialize sliding counter: %v", err)
    }
    if err := dm.Put(context.Background(), fixedKey, 0); err != nil {
        log.Fatalf("Failed to initialize fixed counter: %v", err)
    }

    // Launch goroutines
    for i := 0; i < numRoutines; i++ {
        wg.Add(1)
        go func(routineID int) {
            defer wg.Done()
            
            for j := 0; j < updatesPerRoutine; j++ {
                start := time.Now()
                
                err := updateLimiterState5(context.Background(), dm, userID, endpointID, 1)
                if err != nil {
                    log.Printf("Error in routine %d: %v", routineID, err)
                    continue
                }
                
                latencyChan <- time.Since(start)

                // Occasionally print current state
                if j%20 == 0 {
                    sliding, err := dm.Get(context.Background(), slidingKey)
                    if err != nil {
                        log.Printf("Error getting sliding counter: %v", err)
                    } else {
                        slidingCount, _ := sliding.Int64()
                        fixed, err := dm.Get(context.Background(), fixedKey)
                        if err != nil {
                            log.Printf("Error getting fixed counter: %v", err)
                        } else {
                            fixedCount, _ := fixed.Int64()
                            log.Printf("Routine %d (update %d) state:", routineID, j)
                            log.Printf("  Sliding Window: %d/500", slidingCount)
                            log.Printf("  Fixed Window: %d/500", fixedCount)
                        }
                    }
                }
            }
        }(i)
    }

    // Wait for all routines to complete
    wg.Wait()
    close(latencyChan)

    // Print final state
    fmt.Printf("\nFinal State:\n")
    sliding, err := dm.Get(context.Background(), slidingKey)
    if err != nil {
        log.Printf("Error getting final sliding counter: %v", err)
    } else {
        slidingCount, _ := sliding.Int64()
        fixed, err := dm.Get(context.Background(), fixedKey)
        if err != nil {
            log.Printf("Error getting final fixed counter: %v", err)
        } else {
            fixedCount, _ := fixed.Int64()
            fmt.Printf("Sliding Window: %d/500\n", slidingCount)
            fmt.Printf("Fixed Window: %d/500\n", fixedCount)
        }
    }

    // Calculate statistics
    var totalLatency time.Duration
    var maxLatency time.Duration
    var minLatency = time.Hour
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

    // Shutdown Olric
    ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    if err := db.Shutdown(ctx); err != nil {
        log.Printf("Failed to shutdown Olric: %v", err)
    }
}
