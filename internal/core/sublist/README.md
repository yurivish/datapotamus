# Subject Matcher Package

A high-performance subject-based routing mechanism with wildcard support, extracted from the NATS server implementation.

[Sublist docs](https://pkg.go.dev/github.com/nats-io/nats-server/v2@v2.11.6/server#Sublist)

I ported the tests with Claude's assistance and manually ported the sublist, Then compared both the sublist and the tests with beyond compare to the implementation in the NATS server repo:

- [`sublist.go`](https://github.com/nats-io/nats-server/blob/74a7d883bbf71ae6f6e44eef668946bcc40ca7e0/server/sublist.go)
- [`sublist_test.go`](https://github.com/nats-io/nats-server/blob/74a7d883bbf71ae6f6e44eef668946bcc40ca7e0/server/sublist_test.go)

These original files are stored in the repo as `_nats_sublist.go` and `_nats_sublist_test.go` for reference.

Claude told me that the the `Qsubs` field on the result type is basically an array of subscriber [queue groups](https://docs.nats.io/nats-concepts/core-nats/queue) that have subscribed particular queues. So if you want to do load balancing between those you can just pick a random subscriber from the group and send your message to it.

This port was done on July 22, 2025.

## Optimization

We could tune these cache parameters. The cache pruning eviction strategy is "random".

```go
const (
	// cacheMax is used to bound limit the frontend cache
	slCacheMax = 1024
	// If we run a sweeper we will drain to this count.
	slCacheSweep = 256
	// plistMin is our lower bounds to create a fast plist for Match.
	plistMin = 256
)
```

## Quick Example

Here's a minimal example showing how to use the subject matcher:

```go
package main

import (
    "fmt"
    "datapotamus.com/internal/sublist"
)

func main() {
    // Create a new sublist with caching
    sl := sublist.NewSublistWithCache()

		// Add some subscriptions
		sl	.Insert(&sublist.Subscription{
			Value:   "sensor1",
			Subject: []byte("sensors.temperature.room1"),
		})

		sl.Insert(&sublist.Subscription{
			Value:   "all-temps",
			Subject: []byte("sensors.temperature.*"),
		})

		sl.Insert(&sublist.Subscription{
			Value:   "all-sensors",
			Subject: []byte("sensors.>"),
		})

		sl.Insert(&sublist.Subscription{
			Value:   "no-sensors",
			Subject: []byte("tensors.>"),
		})

    // Match a subject
    result := sl.Match("sensors.temperature.room1")

    fmt.Printf("Found %d matches:\n", len(result.Psubs))
    for _, sub := range result.Psubs {
        fmt.Printf("  - %s (subject: %s)\n", sub.ID, sub.Subject)
    }

    // Output:
    // Found 3 matches:
    //   - sensor1 (subject: sensors.temperature.room1)
    //   - all-temps (subject: sensors.temperature.*)
    //   - all-sensors (subject: sensors.>)
}
