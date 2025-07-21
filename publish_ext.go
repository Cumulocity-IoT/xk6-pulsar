package pulsar

import (
	"fmt"

	"github.com/grafana/sobek"
)

// PublishAsyncForDuration repeatedly publishes a message asynchronously to the specified Pulsar topic
// at the given interval for a specified duration or until the maximum message count is reached.
//
// Parameters:
//   - durationMillis: total duration in milliseconds to keep publishing.
//   - maxMessageCount: maximum number of messages to publish. If set to -1, publishing continues until the duration elapses.
//   - topic: the PULSAR topic to publish to.
//   - message: the message payload to publish.
//   - messageProperties: application defined properties on the message.
//
// Returns:
//   - The number of successful publish operations.
//     If maxMessageCount is -1, messages will be published until the duration elapses, ignoring the message count limit.
func (c *client) PublishAsyncForDuration(
	durationMillis, maxMessageCount int64,
	topic string,
	message string,
	messageProperties map[string]string,
) (int64, error) {
	return c.invokeForDuration(durationMillis, maxMessageCount, func() error {
		return c.Publish(
			topic,
			message,
			messageProperties,
			func(value sobek.Value) (sobek.Value, error) {
				// success: just return the value or log it
				return value, nil
			},
			func(value sobek.Value) (sobek.Value, error) {
				// failure: return an error for tracking
				return nil, fmt.Errorf("publish failed: %v", value)
			},
		)
	})
}

// PublishSyncForDuration repeatedly publishes a message synchronously to the specified Pulsar topic
// at the given interval for a specified duration or until the maximum message count is reached.
//
// Parameters:
//   - durationMillis: total duration in milliseconds to keep publishing.
//   - maxMessageCount: maximum number of messages to publish.
//   - topic: the PULSAR topic to publish to.
//   - message: the message payload to publish.
//   - messageProperties: application defined properties on the message.
//
// Returns:
//   - The number of successful publish operations.
//     If maxMessageCount is -1, messages will be published until the duration elapses, ignoring the message count limit.
func (c *client) PublishSyncForDuration(
	durationMillis, maxMessageCount int64,
	topic string,
	message string,
	messageProperties map[string]string,
) (int64, error) {
	return c.invokeForDuration(durationMillis, maxMessageCount, func() error {
		return c.publishSync(
			topic,
			message,
			messageProperties,
		)
	})
}
