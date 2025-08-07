from cloudevents.events import Event, PulsarBinding
from viaa.configuration import ConfigParser
from viaa.observability import logging

from app.services.pulsar import PulsarClient
from app.services.db import DbClient, SipDelivery


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
