package pulsar

import "github.com/grafana/sobek"

// Subscribe to the given topic for a specified duration and or message count.
// Returns when the duration expires or the message count is reached.
func (c *client) SubscribeForDuration(
	durationMillis, maxMessageCount int64,
	// Topic to consume messages from
	topic string,
	// A regular expression to subscribe to multiple topics under the same namespace
	topicsPattern string,
	// Subscription type, can be "exclusive", "shared", "failover" or "keyshared" (defaults to "keyshared")
	subscriptionType string,
	// Initial position of the cursor, can be "earliest" or "latest" (defaults to "latest")
	initialPosition string,
) error {
	// Register the message listener to manage the message count
	var count int64
	count = maxMessageCount
	c.messageListener = func(ev sobek.Value) (sobek.Value, error) {
		count--
		if maxMessageCount == UnlimitedMessageCount || count > 0 {
			c.subRefCount++ // increment the reference count so the subscription loop continues
		}

		return ev, nil
	}

	if durationMillis > 0 {
		c.subDuration = durationMillis
	}
	return c.subscriptionLoop(topic, topicsPattern, subscriptionType, initialPosition)
}
