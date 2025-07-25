package pulsar

import (
	"fmt"
	"math"
)

// Subscribe to the given topic for a specified duration and or message count.
// Returns when the duration expires or the message count is reached.
func (c *client) SubscribeForDuration(
	durationMillis, maxMessageCount int64,
	// Topic to consume messages from
	topic string,
	// A regular expression to subscribe to multiple topics under the same namespace
	topicsPattern string,
	// Subscription type, can be "Exclusive", "Shared", "Failover" or "Key_Shared" (defaults to "Key_Shared")
	subscriptionType string,
	// Initial position of the cursor, can be "earliest" or "latest" (defaults to "latest")
	initialPosition string,
) error {
	if durationMillis < 0 && maxMessageCount == UnlimitedMessageCount {
		return fmt.Errorf("invalid parameters: either durationMillis or maxMessageCount must be >= 0")
	}

	if maxMessageCount > 0 {
		c.subRefCount = maxMessageCount - 1
	} else {
		c.subRefCount = math.MaxInt64 // if maxMessageCount is -1, we will run indefinitely by setting a very high count
	}

	if durationMillis > 0 {
		c.subDuration = durationMillis
	} else {
		c.subDuration = 0 // if duration is 0, we will run indefinitely
	}

	return c.subscriptionLoop(topic, topicsPattern, subscriptionType, initialPosition)
}
