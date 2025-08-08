from cloudevents.events import Event, EventAttributes, EventOutcome, PulsarBinding
from viaa.configuration import ConfigParser
from viaa.observability import logging

from app.services.pulsar import PulsarClient
from app.services.db import DbClient, SipDelivery

from . import APP_NAME


class EventListener:
    """EventListener is responsible for listening to Pulsar events and processing them."""

    def __init__(self):
        """Initializes the EventListener with configuration, logging, and Pulsar client."""
        config_parser = ConfigParser()
        self.config = config_parser.app_cfg
        self.db_client = DbClient()
        self.log = logging.get_logger(__name__, config=config_parser)
        self.pulsar_client = PulsarClient()

    def handle_incoming_message(self, event: Event):
        """
        Handles an incoming Pulsar event.

        Args:
            event (Event): The incoming event to process.
        """

        # Event attributes
        attributes = event.get_attributes()
        subject = attributes.get("subject")

        # Check if valid
        if not event.has_successful_outcome():
            self.log.info(f"Dropping non successful event: {subject}")
            return

        self.log.info(f"Start handling of {subject}.")

        # Register SIP delivery in database
        s3_event_data = event.get_data()["s3_message"]["Records"][0]["s3"]
        sip_delivery = SipDelivery(
            correlation_id=event.correlation_id,
            s3_bucket=s3_event_data["bucket"]["name"],
            s3_key=s3_event_data["object"]["key"],
            s3_domain=s3_event_data["domain"]["name"],
        )
        self.db_client.insert_sip_delivery(sip_delivery)

        # Write event
        data = {
            "s3_bucket": sip_delivery.s3_bucket,
            "s3_key": sip_delivery.s3_key,
            "s3_domain": sip_delivery.s3_domain
        }
        producer_topic = self.config["pulsar"]["producer_topic"]

        self.produce_event(
            producer_topic, data, subject, EventOutcome.SUCCESS, event.correlation_id
        )

    def produce_event(
        self,
        topic: str,
        data: dict,
        subject: str,
        outcome: EventOutcome,
        correlation_id: str,
    ):
        """Produce an event on a Pulsar topic.
        Args:
            topic: The topic to send the cloudevent to.
            data: The data payload.
            subject: The subject of the event.
            outcome: The attributes outcome of the Event.
            correlation_id: The correlation ID.
        """
        attributes = EventAttributes(
            type=topic,
            source=APP_NAME,
            subject=subject,
            correlation_id=correlation_id,
            outcome=outcome,
        )

        event = Event(attributes, data)
        self.pulsar_client.produce_event(topic, event)

    def start_listening(self):
        """
        Starts listening for incoming messages from the Pulsar topic.
        """
        while True:
            msg = self.pulsar_client.receive()
            try:
                event = PulsarBinding.from_protocol(msg)  # type: ignore
                self.handle_incoming_message(event)
                self.pulsar_client.acknowledge(msg)
            except Exception as e:
                # Catch and log any errors during message processing
                self.log.error(f"Error: {e}")
                self.pulsar_client.negative_acknowledge(msg)

        self.pulsar_client.close()
