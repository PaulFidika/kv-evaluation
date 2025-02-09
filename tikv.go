package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv"
)

// LimiterState2 holds the current count and a maximum allowed count (limit).
type LimiterState2 struct {
    Count int `json:"count"`
    Limit int `json:"limit"`
}

// updateLimiterState8 performs one transactional update on the limiter state key.
// It returns (limitReached, error). If limitReached is true, the update was not done
// because the limit was already reached/exceeded.
func updateLimiterState8(ctx context.Context, client *txnkv.Client, key []byte) (bool, error) {
    const maxRetries = 3
    for attempt := 1; attempt <= maxRetries; attempt++ {
        // Begin a new transaction (pessimistic mode)
        txn, err := client.Begin()
        if err != nil {
            return false, fmt.Errorf("begin txn failed: %w", err)
        }
        txn.SetPessimistic(true) // enable pessimistic locking on this transaction

        // Lock the key to prevent concurrent writes to it (wait indefinitely for the lock)
        err = txn.LockKeysWithWaitTime(ctx, kv.LockAlwaysWait, key)
        if err != nil {
            txn.Rollback() // rollback to release any partial locks
            return false, fmt.Errorf("failed to lock key: %w", err)
        }

        // Get the current value of the limiter state
        value, err := txn.Get(ctx, key)
        if err != nil {
            txn.Rollback()
            return false, fmt.Errorf("failed to get current value: %w", err)
        }
        if value == nil {
            // Key should exist since we initialized it; if not, rollback and error
            txn.Rollback()
            return false, fmt.Errorf("key not found")
        }

        // Deserialize JSON into LimiterState2
        var state LimiterState2
        if err := json.Unmarshal(value, &state); err != nil {
            txn.Rollback()
            return false, fmt.Errorf("failed to decode JSON: %w", err)
        }

        // Check rate limit
        if state.Count >= state.Limit {
            // The limit has been reached; do not update further.
            txn.Rollback() // release the lock since we won't commit
            return true, nil // indicate limit reached (not an error, but no update done)
        }

        // Increment the counter (within limit) and serialize back to JSON
        state.Count++
        newData, err := json.Marshal(state)
        if err != nil {
            txn.Rollback()
            return false, fmt.Errorf("failed to encode JSON: %w", err)
        }
        if err := txn.Set(key, newData); err != nil {
            txn.Rollback()
            return false, fmt.Errorf("failed to set new value: %w", err)
        }

        // Commit the transaction to apply the changes
        err = txn.Commit(ctx)
        if err != nil {
            // If a write conflict or commit error occurs, decide whether to retry
            txn.Rollback() // ensure any locks are freed
            if tikverr.IsErrWriteConflict(err) && attempt < maxRetries {
                // Conflict detected, retry the transaction
                continue  // try again in a new transaction
            }
            // For non-retryable errors or max retries exceeded, return error
            return false, fmt.Errorf("transaction commit failed: %w", err)
        }

        // Success - the transaction committed
        return false, nil
    }
    // If we exit the loop, it means we retried and still failed
    return false, fmt.Errorf("update failed after multiple retries")
}

func main4() {
    ctx := context.Background()

    // 1. Create TiKV client and initialize data
    client, err := txnkv.NewClient([]string{"127.0.0.1:2379"})
    if err != nil {
        panic(fmt.Errorf("failed to connect to TiKV: %w", err))
    }
    defer client.Close()

    key := []byte("limiter_state")
    initial := LimiterState2{Count: 0, Limit: 100}
    initData, _ := json.Marshal(initial)

    // Store initial state in TiKV under 'limiter_state' key (within a transaction)
    txn, err := client.Begin()
    if err != nil {
        panic(fmt.Errorf("failed to begin init txn: %w", err))
    }
    if err := txn.Set(key, initData); err != nil {
        panic(fmt.Errorf("failed to set initial value: %w", err))
    }
    if err := txn.Commit(ctx); err != nil {
        panic(fmt.Errorf("failed to commit initial value: %w", err))
    }
    fmt.Println("Initialized limiter state in TiKV:", initial)

    // 2. Launch 10 goroutines, each performing 10 updates concurrently
    var wg sync.WaitGroup
    numGoroutines := 10
    updatesPerGoroutine := 10

    wg.Add(numGoroutines)
    for i := 1; i <= numGoroutines; i++ {
        go func(id int) {
            defer wg.Done()
            for j := 1; j <= updatesPerGoroutine; j++ {
                limitReached, err := updateLimiterState8(ctx, client, key)
                if err != nil {
                    // Handle error (could log and break, here we print for demo)
                    fmt.Printf("Goroutine %d: update %d failed: %v\n", id, j, err)
                    break // stop on error
                }
                if limitReached {
                    // If limit is reached, stop further updates in this goroutine
                    fmt.Printf("Goroutine %d: rate limit reached, stopping updates.\n", id)
                    break
                }
                // (Optional) print progress
                // fmt.Printf("Goroutine %d: successfully performed update %d\n", id, j)
            }
        }(i)
    }

    // Wait for all goroutines to finish their updates
    wg.Wait()

    // 3. After concurrency, read the final state from TiKV to verify results
    readTxn, err := client.Begin() // new transaction (default optimistic) for reading
    if err != nil {
        panic(fmt.Errorf("failed to begin read txn: %w", err))
    }
    defer readTxn.Rollback() // no need to commit a read-only transaction (or we can just drop it)
    finalVal, err := readTxn.Get(ctx, key)
    if err != nil {
        panic(fmt.Errorf("failed to read final value: %w", err))
    }
    var finalState LimiterState2
    if err := json.Unmarshal(finalVal, &finalState); err != nil {
        panic(fmt.Errorf("failed to decode final JSON: %w", err))
    }
    fmt.Println("Final limiter state:", finalState)
}

