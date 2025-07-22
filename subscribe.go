package pulsar

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/metrics"
)

// Subscribe to the given topic message will be received using addEventListener
func (c *client) Subscribe(
	// Topic to consume messages from
	topic string,
	// A regular expression to subscribe to multiple topics under the same namespace
	topicsPattern string,
	// Subscription type, can be "exclusive", "shared", "failover" or "keyshared" (defaults to "keyshared")
	subscriptionType string,
	// Initial position of the cursor, can be "earliest" or "latest" (defaults to "latest")
	initialPosition string,
) error {
	c.subRefCount = 0 // reset the reference count for subscribe
	c.subDuration = 0 // reset the duration for subscribe

	go c.subscriptionLoop(topic, topicsPattern, subscriptionType, initialPosition)
	return nil
}

func (c *client) createConsumer(topic, topicsPattern, subscriptionType, initialPosition string) (pulsar.Consumer, error) {
	if topic != "" && topicsPattern != "" {
		return nil, fmt.Errorf("both topic and topicsPattern are set, only one should be used")
	}

	var subscriptionName string
	if topicsPattern != "" {
		subscriptionName = topicsPattern
	} else {
		subscriptionName = topic
	}

	if c.pulsarConsumer != nil && c.pulsarConsumer.Subscription() == subscriptionName {
		// if the consumer is already created for the topic, just return it
		return c.pulsarConsumer, nil
	} else if c.pulsarConsumer != nil {
		return nil, fmt.Errorf("cannot create multiple consumers using one client - consumer already exists for topic %s, cannot create another one for %s", c.pulsarConsumer.Subscription(), subscriptionName)
	}

	opts := pulsar.ConsumerOptions{
		Topic:                       topic,
		TopicsPattern:               topicsPattern,
		Name:                        c.conf.name,
		SubscriptionName:            subscriptionName,
		Type:                        stringToSubscriptionType(subscriptionType),
		SubscriptionInitialPosition: stringToSubscriptionInitialPosition(initialPosition),
	}

	consumer, err := c.pulsarClient.Subscribe(opts)
	if err != nil {
		return nil, err
	}
	c.pulsarConsumer = consumer

	return consumer, nil
}

//nolint:gocognit // todo improve this
func (c *client) subscriptionLoop(topic, topicsPattern, subscriptionType, initialPosition string) error {
	rt := c.vu.Runtime()
	consumer, err := c.createConsumer(topic, topicsPattern, subscriptionType, initialPosition)
	if err != nil {
		err = errors.Join(ErrConnect, err)
		common.Throw(rt, err)
		return err
	}

	ctx := c.vu.Context()

	var timeoutChan <-chan time.Time
	if c.subDuration > 0 {
		timeoutChan = time.After(time.Millisecond * time.Duration(c.subDuration))
	}

	for {
		select {
		case msg, ok := <-consumer.Chan():
			if !ok {
				// wanted exit in case of chan close
				return nil
			}

			// ACK the message
			if err := consumer.Ack(msg); err != nil {
				if c.errorListener != nil {
					ev := c.newErrorEvent(fmt.Errorf("failed to ack message: %w", err).Error())
					if _, err := c.errorListener(ev); err != nil {
						// only seen in case of sigint
						return err
					}
				}
			}

			// publish associated metric
			err := c.receiveMessageMetric(float64(len(msg.Payload())))
			if err != nil {
				return err
			}

			if c.messageListener != nil {
				payload := string(msg.Payload())
				ev := c.newMessageEvent(msg.Topic(), payload)
				if _, err := c.messageListener(ev); err != nil {
					return err
				}
			}

			// if the client is waiting for multiple messages
			// TODO handle multiple // subscribe case
			if c.subRefCount > 0 {
				c.subRefCount--
			} else {
				return nil // exit the loop if no more messages are expected
			}

		case <-ctx.Done():
			if c.errorListener != nil {
				ev := c.newErrorEvent("message vu cancel occurred")
				if _, err := c.errorListener(ev); err != nil {
					// only seen in case of sigint
					return err
				}
			}

			// exit the handle from evloop async
			return nil

		case <-timeoutChan:
			// exit the handle from evloop async
			return nil
		}
	}
}

func (c *client) receiveMessageMetric(msgLen float64) error {
	// publish metrics
	now := time.Now()
	state := c.vu.State()
	if state == nil {
		return ErrState
	}

	ctx := c.vu.Context()
	if ctx == nil {
		return ErrState
	}
	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: c.metrics.ReceivedMessages, Tags: c.metrics.TagsAndMeta.Tags},
		Time:       now,
		Value:      float64(1),
	})
	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: c.metrics.ReceivedBytes, Tags: c.metrics.TagsAndMeta.Tags},
		Time:       now,
		Value:      msgLen,
	})
	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: c.metrics.ReceivedDates, Tags: c.metrics.TagsAndMeta.Tags},
		Time:       now,
		Value:      float64(now.UnixMilli()),
	})
	return nil
}

// AddEventListener expose the js method to listen for events
func (c *client) AddEventListener(event string, listener func(sobek.Value) (sobek.Value, error)) {
	switch event {
	case "message":
		c.messageListener = listener
	case "error":
		c.errorListener = listener
	default:
		rt := c.vu.Runtime()
		common.Throw(rt, errors.New("event: "+event+" does not exists"))
	}
}

// SubContinue to be call in message callback to wait for on more message
// be careful this must be called only in the event loop and it not thread safe
func (c *client) SubContinue() {
	c.subRefCount++
}

//nolint:nosnakecase // their choice not mine
func (c *client) newMessageEvent(topic, msg string) *sobek.Object {
	rt := c.vu.Runtime()
	o := rt.NewObject()
	must := func(err error) {
		if err != nil {
			common.Throw(rt, err)
		}
	}

	must(o.DefineDataProperty("topic", rt.ToValue(topic), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(o.DefineDataProperty("message", rt.ToValue(msg), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	return o
}

// stringToSubscriptionType converts a string to a SubscriptionType constant.
// It returns the corresponding constant.
func stringToSubscriptionType(s string) pulsar.SubscriptionType {
	// Convert the input string to lowercase for case-insensitive matching
	lowerS := strings.ToLower(s)

	switch lowerS {
	case "exclusive":
		return pulsar.Exclusive
	case "shared":
		return pulsar.Shared
	case "failover":
		return pulsar.Failover
	case "keyshared":
		return pulsar.KeyShared
	default:
		// If the string doesn't match any known constant, return pulsar.KeyShared value
		return pulsar.KeyShared
	}
}

// stringToSubscriptionInitialPosition converts a string to a SubscriptionInitialPosition constant.
// It returns the corresponding constant.
func stringToSubscriptionInitialPosition(s string) pulsar.SubscriptionInitialPosition {
	// Convert the input string to lowercase for case-insensitive matching
	lowerS := strings.ToLower(s)

	switch lowerS {
	case "earliest":
		return pulsar.SubscriptionPositionEarliest
	case "latest":
		return pulsar.SubscriptionPositionLatest
	default:
		// If the string doesn't match any known constant, return pulsar.SubscriptionPositionLatest value
		return pulsar.SubscriptionPositionLatest
	}
}
