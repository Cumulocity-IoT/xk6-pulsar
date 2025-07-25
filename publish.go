package pulsar

import (
	"errors"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/metrics"
)

// Publish allow to publish one message
//
//nolint:gocognit
func (c *client) Publish(
	topic string,
	message string,
	messageProperties map[string]string,
	success func(sobek.Value) (sobek.Value, error),
	failure func(sobek.Value) (sobek.Value, error),
) error {
	// sync case no callback added
	if success == nil && failure == nil {
		return c.publishSync(topic, message, messageProperties)
	}

	// async case
	callback := c.vu.RegisterCallback()
	publisher, err := c.createProducer(topic)
	if err != nil {
		callback(func() error {
			if failure != nil {
				ev := c.newErrorEvent(fmt.Sprintf("publisher not connected: %w", err))
				if _, err := failure(ev); err != nil {
					return err
				}
			}
			return nil
		})
		return err
	}

	ctx := c.vu.Context()
	if ctx == nil {
		callback(func() error {
			if failure != nil {
				ev := c.newErrorEvent(ErrState.Error())
				if _, err := failure(ev); err != nil {
					return err
				}
			}
			return nil
		})
		return nil
	}
	publisher.SendAsync(ctx,
		&pulsar.ProducerMessage{
			Payload:    []byte(message),
			Properties: messageProperties,
		},
		func(id pulsar.MessageID, msg *pulsar.ProducerMessage, err error) {
			if err != nil {
				callback(func() error {
					if failure != nil {
						ev := c.newErrorEvent(errors.Join(ErrPublish, err).Error())
						if _, err := failure(ev); err != nil {
							return err
						}
					}
					return nil
				})
				return
			}

			callback(func() error {
				err := c.publishMessageMetric(float64(len(message)))
				if err != nil {
					return err
				}
				ev := c.newPublishEvent(topic)
				if success != nil {
					if _, err := success(ev); err != nil {
						return err
					}
				}
				return nil
			})
		},
	)

	return nil
}

func (c *client) publishSync(
	topic string,
	message string,
	messageProperties map[string]string,
) error {
	rt := c.vu.Runtime()
	publisher, err := c.createProducer(topic)
	if err != nil {
		err = errors.Join(ErrConnect, err)
		common.Throw(rt, err)
		return err
	}

	ctx := c.vu.Context()
	if ctx == nil {
		return ErrState
	}
	_, err = publisher.Send(ctx, &pulsar.ProducerMessage{
		Payload:    []byte(message),
		Properties: messageProperties,
	})
	if err != nil {
		rt := c.vu.Runtime()
		err = errors.Join(ErrPublish, err)
		common.Throw(rt, err)
		return err
	}

	err = c.publishMessageMetric(float64(len(message)))
	if err != nil {
		return err
	}
	return nil
}

func (c *client) createProducer(topic string) (pulsar.Producer, error) {
	if c.pulsarProducer != nil && c.pulsarProducer.Topic() == topic {
		// if the producer is already created for the topic, just return it
		return c.pulsarProducer, nil
	} else if c.pulsarProducer != nil {
		return nil, fmt.Errorf("cannot create multiple producers using one client - producer already exists for topic %s, cannot create another one for %s", c.pulsarProducer.Topic(), topic)
	}

	opts := pulsar.ProducerOptions{
		Topic:       topic,
		Name:        c.conf.name,
		SendTimeout: time.Duration(c.conf.publishTimeout) * time.Millisecond,
	}

	producer, err := c.pulsarClient.CreateProducer(opts)
	if err != nil {
		return nil, err
	}
	c.pulsarProducer = producer

	return producer, nil
}

func (c *client) publishMessageMetric(msgLen float64) error {
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
		TimeSeries: metrics.TimeSeries{Metric: c.metrics.SentMessages, Tags: c.metrics.TagsAndMeta.Tags},
		Time:       now,
		Value:      float64(1),
	})
	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: c.metrics.SentBytes, Tags: c.metrics.TagsAndMeta.Tags},
		Time:       now,
		Value:      msgLen,
	})
	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		TimeSeries: metrics.TimeSeries{Metric: c.metrics.SentDates, Tags: c.metrics.TagsAndMeta.Tags},
		Time:       now,
		Value:      float64(now.UnixMilli()),
	})
	return nil
}

//nolint:nosnakecase // their choice not mine
func (c *client) newPublishEvent(topic string) *sobek.Object {
	rt := c.vu.Runtime()
	o := rt.NewObject()
	must := func(err error) {
		if err != nil {
			common.Throw(rt, err)
		}
	}

	must(o.DefineDataProperty("type", rt.ToValue("publish"), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(o.DefineDataProperty("topic", rt.ToValue(topic), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	return o
}
