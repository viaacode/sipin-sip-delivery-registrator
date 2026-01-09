from cloudevents.events import CEMessageMode, Event, PulsarBinding
from pulsar import Client
from viaa.configuration import ConfigParser
from viaa.observability import logging

from .. import APP_NAME


class PulsarClient:
    """
    Abstraction for a Pulsar Client.
    """

    def __init__(self, config_parser: ConfigParser):
        """Initialize the PulsarClient with configurations and a consumer."""
        self.log = logging.get_logger(__name__, config=config_parser)
        self.pulsar_config = config_parser.app_cfg["pulsar"]

        self.client = Client(
            f"pulsar://{self.pulsar_config['host']}:{self.pulsar_config['port']}"
        )
        self.consumer = self.client.subscribe(
            self.pulsar_config["consumer_topic"], APP_NAME
        )
        self.log.info(
            f"Started consuming topic: {self.pulsar_config['consumer_topic']}"
        )
        self.producers = {}

    def produce_event(self, topic: str, event: Event):
        """Produce a CloudEvent on a specified topic.

        If no producer exists for the topic, a new one is created.

        Args:
            topic (str): The topic to send the CloudEvent to.
            event (Event): The CloudEvent to send.
        """
        if topic not in self.producers:
            self.producers[topic] = self.client.create_producer(topic)

        msg = PulsarBinding.to_protocol(event, CEMessageMode.STRUCTURED)
        self.producers[topic].send(
            msg.data,
            properties=msg.attributes,
            event_timestamp=event.get_event_time_as_int(),
        )

    def receive(self):
        """Receive a message from the consumer.

        Returns:
            Message: The received message.
        """
        return self.consumer.receive()

    def acknowledge(self, msg):
        """Acknowledge a message on the consumer.

        Args:
            msg: The message to acknowledge.
        """
        self.consumer.acknowledge(msg)

    def negative_acknowledge(self, msg):
        """Send a negative acknowledgment (nack) for a message.

        Args:
            msg: The message to nack.
        """
        self.consumer.negative_acknowledge(msg)

    def close(self):
        """Close all producers and the consumer."""
        for producer in self.producers.values():
            producer.close()
        self.consumer.close()
