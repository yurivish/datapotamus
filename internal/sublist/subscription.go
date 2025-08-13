package sublist

// Subscription represents a subscription to a subject pattern.
// It's a minimal representation suitable for routing without NATS-specific concerns.
type Subscription struct {
	// Value is an arbitrary identifier for the subscription, can be any type
	Value any

	// Subject is the subject pattern this subscription matches
	Subject []byte

	// Queue is an optional queue group name
	Queue []byte

	// File and line of the source code location where the subscription was created.
	// TODO: How can we strip this metadata out of "production" builds?
	File     string
	Line     int
	FuncName string

	// So we can identify individual subscriptions
	ID string

	// So we can tell if this is a "Debug" subscription (created with DebugSub)
	Debug bool
}
