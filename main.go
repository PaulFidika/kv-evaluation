package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
	openmeter "github.com/openmeterio/openmeter/api/client/go"
)

type ImageGenService struct {
    client *openmeter.ClientWithResponses
    feature string
    subject string
}

func NewImageGenService(apiKey string) (*ImageGenService, error) {
	client, err := openmeter.NewAuthClientWithResponses("https://openmeter.cloud", apiKey)
    if err != nil {
        return nil, fmt.Errorf("failed to create client: %w", err)

    }

    return &ImageGenService{
        client: client,
        feature: "image-gen-endpoint",
        subject: "customer-123",
    }, nil
}

// Meters are better called 'counters', and instead of metering we are 'logging usage'.
// 'Meters' are just usage logs.
func (s *ImageGenService) LogUsage(ctx context.Context) (bool, error) {
	// TO DO: 
	// create counters (meters)
	// create a customer-id (subject)
	// create a limit for each counter + customer-id (there are no policies? It's per customer?)
	// ^^ These are called entitlements ^^
	// 
	// Unfortunately, gating is done per FEATURE, not per COUNTER. You would think that
	// for a given event, I would have 3 counters (a unique count per minute, unique count per
	// 3 hours, and then a token count for the day, for example) and then place LIMITs on each
	// of these. However this is not the case.
	// Instead, what you would do is create 3 counters, sharing the same event-type.
	// THEN you would create 3 features--one for each counter.
	// THEN you would give the customer access to all 3 features (3 entitlements)
	// 'entitlement' is better named 'limit' (where an undefined limit means inaccessible).
	// each counter gets a limit. Then you would check access for each limit INDIVIDUALLY
	// so that would be 3 API calls.
	//
	// There does not appear to be any notion of a POLICY, i.e., you must manually create 
	// entitlements (limits) per user, per feature, rather than assigning a user to a 'plan'.
	//
	// Unfortunately we are also missing (1) concurrency counts, and (2) quota (credits)
	// Tyke has a single rate-limiter, throttling, and quota.

	e := cloudevents.New()

	e.SetTime(time.Now())

	// Use unique IDs for each event.
	// Let's call this request ID
	e.SetID(uuid.NewString())

	// Set the event source for an identifier of the reporting service.
	// Events are deduplicated based on the source and ID.
	e.SetSource("cozy-creator")

	// This is better called 'endpointID'. When you create a meter, you can set its 'event type'.
	// So if you log an event with type 'image-gen' then all meters with that event-type will be
	// updated.
	// More abstractly, you could think of this as an 'eventType', and meters operating on
	// the number of allowed occurences of that event, such as a 'failed generation' meter; you
	// might sent the event-type as 'failed-genreation', and create a meter which only allows
	// 5 failed genreations per hour.
	e.SetType("text2media")

	// Set the event subject (e.g. the customer ID). Meter values are aggregated by subjects.
	// This is better called 'customerID'.
	e.SetSubject("customer123")

	// Set the event data, including the valueProperty and groupBy fields.
	e.SetData("application/json", map[string]string{
		"gpu_time": "30",
		"outputs": "4",
	})
	
	resp, err := s.client.IngestEventWithResponse(ctx, e)

	// Handle errors.
	if err != nil {
		return false, fmt.Errorf("failed to log usage: %w", err)
	}

	// Handle non-2xx status codes.
	// An error is returned if caused by client policy (such as CheckRedirect),
	// or failure to speak HTTP (such as a network connectivity problem).
	// A non-2xx status code doesn't cause an error.
	// See: https://pkg.go.dev/net/http#Client.Do
	if resp.StatusCode() >= 400 {
		return false, fmt.Errorf("non-2xx status code: %d", resp.StatusCode())
	}

	return true, nil
}

func (s *ImageGenService) CheckAvailability() (bool, float64, float64, float64, error)  {
	ctx := context.Background()

	// we are only checking for GPU time, not count of outputs or count of requests
	entitlement, err := s.client.GetEntitlementValueWithResponse(ctx, "customer123", "gputimecheck", &openmeter.GetEntitlementValueParams{})
	if err != nil {
		return false, 0, 0, 0, fmt.Errorf("failed to get entitlement: %w", err)
	}

	hasAccess := entitlement.JSON200.HasAccess
	balance := entitlement.JSON200.Balance
	// config := entitlement.JSON200.Config
	overage := entitlement.JSON200.Overage
	usage := entitlement.JSON200.Usage

	// fmt.Println(hasAccess, balance, config, overage, usage)

	return *hasAccess, *balance, *overage, *usage, nil
}


// Example usage in your HTTP handler
// func handleImageGenRequest(svc *ImageGenService) http.HandlerFunc {
//     return func(w http.ResponseWriter, r *http.Request) {
//         allowed, err := svc.CheckAvailability()
//         if err != nil {
//             http.Error(w, "Error checking usage", http.StatusInternalServerError)
//             return
//         }

//         if !allowed {
//             http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
//             return
//         }

//         // Continue with image generation...
//     }
// }

func main2() {
	if err := godotenv.Load(); err != nil {
        log.Printf("Warning: Error loading .env file: %v", err)
    }

 svc, err := NewImageGenService(os.Getenv("TOKEN"))
    if err != nil {
        log.Fatalf("failed to create service: %v", err)
    }

    // Test parameters
    iterations := 10
    ctx := context.Background()

    // Test LogUsage
    fmt.Println("\n=== Testing LogUsage ===")
    var totalLogUsageTime time.Duration
    for i := 0; i < iterations; i++ {
        start := time.Now()
        success, err := svc.LogUsage(ctx)
        elapsed := time.Since(start)
        totalLogUsageTime += elapsed

        fmt.Printf("Iteration %d: Latency: %v, Success: %v, Error: %v\n", 
            i+1, elapsed, success, err)
        
        // Small delay between requests to avoid overwhelming the API
        time.Sleep(time.Millisecond * 100)
    }

    // Test CheckAvailability
    fmt.Println("\n=== Testing CheckAvailability ===")
    var totalCheckTime time.Duration
    for i := 0; i < iterations; i++ {
        start := time.Now()
        hasAccess, balance, overage, usage, err := svc.CheckAvailability()
        elapsed := time.Since(start)
        totalCheckTime += elapsed

        fmt.Printf("Iteration %d: Latency: %v\n", i+1, elapsed)
        fmt.Printf("  HasAccess: %v, Balance: %.2f, Overage: %.2f, Usage: %.2f, Error: %v\n",
            hasAccess, balance, overage, usage, err)
        
        time.Sleep(time.Millisecond * 100)
    }

    // Print averages
    fmt.Printf("\n=== Results ===\n")
    fmt.Printf("Average LogUsage latency: %v\n", 
        totalLogUsageTime/time.Duration(iterations))
    fmt.Printf("Average CheckAvailability latency: %v\n", 
        totalCheckTime/time.Duration(iterations))
}
