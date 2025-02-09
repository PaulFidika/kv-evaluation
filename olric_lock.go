package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/buraksezer/olric"
	"github.com/buraksezer/olric/config"
)

const (
	numRoutines    = 10
	updatesPerRoutine = 100
	limit         = 1000
	lockTimeout   = 1 * time.Second
)

func incrementWithLock(ctx context.Context, dm olric.DMap, key string, amount int64, routineID int) error {
	// Try to acquire lock
	token, err := dm.LockWithTimeout(ctx, key, 1 * time.Second, lockTimeout)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	
	// Ensure we release the lock
	defer func() {
		if err := token.Unlock(ctx); err != nil {
			log.Printf("routine %d failed to release lock: %v", routineID, err)
		}
	}()

	// Read current value
	val, err := dm.Get(ctx, key)
	if err != nil && err != olric.ErrKeyNotFound {
		return fmt.Errorf("failed to get value: %w", err)
	}

	// Get current count, defaulting to 0 if not found
	var currentCount int64
	if val != nil {
		currentCount, err = val.Int64()
		if err != nil {
			return fmt.Errorf("failed to parse value: %w", err)
		}
	}

	// Check if adding amount would exceed limit
	if currentCount+amount > limit {
		return fmt.Errorf("would exceed limit of %d", limit)
	}

	// Increment the value
	err = dm.Put(ctx, key, currentCount+amount)
	if err != nil {
		return fmt.Errorf("failed to put new value: %w", err)
	}

	return nil
}

func main6() {
	// Create Olric config
	c := config.New("local")

	// Setup callback for when Olric is ready
	ctx, cancel := context.WithCancel(context.Background())
	c.Started = func() {
		defer cancel()
		log.Println("[INFO] Olric is ready to accept connections")
	}

	// Create and start Olric instance
	db, err := olric.New(c)
	if err != nil {
		log.Fatalf("Failed to create Olric instance: %v", err)
	}

	go func() {
		if err := db.Start(); err != nil {
			log.Fatalf("olric.Start returned an error: %v", err)
		}
	}()

	<-ctx.Done()

	// Create embedded client
	client := db.NewEmbeddedClient()

	// Create DMap
	dm, err := client.NewDMap("counter")
	if err != nil {
		log.Fatalf("Failed to create DMap: %v", err)
	}

	// Initialize counter to 0
	ctx = context.Background()
	key := "count"
	err = dm.Put(ctx, key, int64(0))
	if err != nil {
		log.Fatalf("Failed to initialize counter: %v", err)
	}

	// Create wait group for goroutines
	var wg sync.WaitGroup
	startTime := time.Now()

	// Launch goroutines
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			
			for j := 0; j < updatesPerRoutine; j++ {
				err := incrementWithLock(ctx, dm, key, 1, routineID)
				if err != nil {
					log.Printf("Routine %d update %d failed: %v", routineID, j, err)
					continue
				}
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	duration := time.Since(startTime)

	// Get final value
	val, err := dm.Get(ctx, "count")
	if err != nil {
		log.Fatalf("Failed to get final value: %v", err)
	}
	
	finalCount, err := val.Int64()
	if err != nil {
		log.Fatalf("Failed to parse final value: %v", err)
	}

	// Print results
	fmt.Printf("\nTest Results:\n")
	fmt.Printf("Total Operations Attempted: %d\n", numRoutines*updatesPerRoutine)
	fmt.Printf("Final Counter Value: %d\n", finalCount)
	fmt.Printf("Total Time: %v\n", duration)
	fmt.Printf("Operations/sec: %.2f\n", float64(numRoutines*updatesPerRoutine)/duration.Seconds())

	// Shutdown Olric
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.Shutdown(ctx); err != nil {
		log.Printf("Failed to shutdown Olric: %v", err)
	}
}

