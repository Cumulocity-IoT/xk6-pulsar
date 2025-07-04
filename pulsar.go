package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

func (p *Pulsar) Connect(url string) error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: url,
	})
	if err != nil {
		return err
	}
	p.client = client
	return nil
}

func (p *Pulsar) IsConnected() bool {
	return p.client != nil
}

func (p *Pulsar) Send(topic string, message string) error {
	if p.producer == nil {
		producer, err := p.client.CreateProducer(pulsar.ProducerOptions{
			Topic: topic,
		})
		if err != nil {
			return err
		}

		p.producer = producer
	}

	_, err := p.producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte(message),
	})

	return err
}

func (p *Pulsar) IsProducerConnected() bool {
	return p.producer != nil
}

func (p *Pulsar) Receive(topic, sub string) (string, error) {
	if p.consumer == nil {
		consumer, err := p.client.Subscribe(pulsar.ConsumerOptions{
			Topic:            topic,
			SubscriptionName: sub,
			Type:             pulsar.Shared,
		})
		if err != nil {
			return "", err
		}
		p.consumer = consumer
	}

	msg, err := p.consumer.Receive(context.Background())
	if err != nil {
		return "", err
	}

	p.consumer.Ack(msg)

	return string(msg.Payload()), nil
}

func (p *Pulsar) IsConsumerConnected() bool {
	return p.consumer != nil
}

func (p *Pulsar) Close() {
	if p.producer != nil {
		p.producer.Close()
		p.producer = nil
	}

	if p.consumer != nil {
		p.consumer.Close()
		p.consumer = nil
	}

	if p.client != nil {
		p.client.Close()
		p.client = nil
	}
}
