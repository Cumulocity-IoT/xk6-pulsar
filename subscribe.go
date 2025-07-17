package pulsar

import (
	"errors"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grafana/sobek"
	"github.com/mstoykov/k6-taskqueue-lib/taskqueue"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/metrics"
)

func (c *client) createConsumerIfNotPresent(topic, topicsPattern string) (pulsar.Consumer, error) {
	if topic != "" && topicsPattern != "" {
		return nil, fmt.Errorf("both topic and topicsPattern are set, only one should be used")
	}

	var consumerKey string
	if topicsPattern != "" {
		consumerKey = topicsPattern
	} else {
		consumerKey = topic
	}

	var err error
	var ok bool
	var consumer pulsar.Consumer
	consumer, ok = c.pulsarConsumers[consumerKey]
	if !ok {
		c.pulsarConsumersMU.Lock()
		defer c.pulsarConsumersMU.Unlock()

		consumer, ok = c.pulsarConsumers[consumerKey]
		if !ok {
			opts := pulsar.ConsumerOptions{
				Topic:            topic,
				TopicsPattern:    topicsPattern,
				Name:             c.conf.name,
				SubscriptionName: c.conf.name,
			}

			consumer, err = c.pulsarClient.Subscribe(opts)
			if err != nil {
				return nil, err
			}

			c.pulsarConsumers[consumerKey] = consumer
		}
	}

	return consumer, nil
}

// Subscribe to the given topic message will be received using addEventListener
func (c *client) Subscribe(
	// Topic to consume messages from
	topic string,
	// A regular expression to subscribe to multiple topics under the same namespace
	topicsPattern string,
) error {
	rt := c.vu.Runtime()
	consumer, err := c.createConsumerIfNotPresent(topic, topicsPattern)
	if err != nil {
		err = errors.Join(ErrConnect, err)
		common.Throw(rt, err)
		return err
	}

	registerCallback := func() func(func() error) {
		callback := c.vu.RegisterCallback()
		return func(f func() error) {
			callback(f)
		}
	}
	c.tq = taskqueue.New(registerCallback)
	go c.loop(consumer)
	return nil
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

//nolint:gocognit // todo improve this
func (c *client) loop(consumer pulsar.Consumer) {
	ctx := c.vu.Context()
	stop := make(chan struct{})
	defer c.tq.Close()
	for {
		select {
		case msg, ok := <-consumer.Chan():
			if !ok {
				// wanted exit in case of chan close
				return
			}
			c.tq.Queue(func() error {
				payload := string(msg.Payload())

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
				err := c.receiveMessageMetric(float64(len(payload)))
				if err != nil {
					return err
				}

				if c.messageListener != nil {
					ev := c.newMessageEvent(msg.Topic(), payload)
					if _, err := c.messageListener(ev); err != nil {
						return err
					}
				}

				return nil
			})
		case <-stop:
			return
		case <-ctx.Done():
			c.tq.Queue(func() error {
				if c.errorListener != nil {
					ev := c.newErrorEvent("message vu cancel occurred")
					if _, err := c.errorListener(ev); err != nil {
						// only seen in case of sigint
						return err
					}
				}
				return nil
			})
			return
		}
	}
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
