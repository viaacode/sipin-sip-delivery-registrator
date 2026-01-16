import signal
from types import FrameType
from cloudevents.events import Event, EventAttributes, EventOutcome, PulsarBinding
from viaa.configuration import ConfigParser
from viaa.observability import logging

from app.services.db import DbClient, DuplicateKeyError, SipDelivery
from app.services.pulsar import PulsarClient, Timeout

from . import APP_NAME

RECEIVE_MESSAGE_TIMEOUT = 60 * 1000


class EventListener:
    """EventListener is responsible for listening to Pulsar events and processing them."""

    def __init__(self):
        """Initializes the EventListener with configuration, logging, and Pulsar client."""
        config_parser = ConfigParser()
        self.config = config_parser.app_cfg
        self.db_client = DbClient(config_parser)
        self.log = logging.get_logger(__name__, config=config_parser)
        self.pulsar_client = PulsarClient(config_parser)
        self.should_continue = True

    def _stop(self, _signum: int | None, _frame: FrameType | None):
        """Stops the start_listening loop once current event has been processed."""
        self.log.info(
            f"{EventListener.__name__} received a stop signal. "
            "Attempting to shut down gracefully."
        )
        self.should_continue = False

    def _build_payload_event(
        self, sip_delivery: SipDelivery, message: str | None = None
    ) -> dict[str, str]:
        payload = {
            "s3_bucket": sip_delivery.s3_bucket,
            "s3_object_key": sip_delivery.s3_object_key,
            "s3_domain": sip_delivery.s3_domain,
        }

        if message:
            payload["message"] = message

        return payload

    def handle_incoming_message(self, event: Event):
        """
        Handles an incoming Pulsar event.

        Args:
            event (Event): The incoming event to process.
        """

        # Producer
        producer_topic = self.config["pulsar"]["producer_topic"]

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
            s3_object_key=s3_event_data["object"]["key"],
            s3_domain=s3_event_data["domain"]["name"],
        )
        try:
            self.db_client.insert_sip_delivery(sip_delivery)
        except DuplicateKeyError as e:
            self.log.error(f"Error: {str(e)}")
            data = self._build_payload_event(sip_delivery, str(e))
            self.produce_event(
                producer_topic, data, subject, EventOutcome.FAIL, event.correlation_id
            )
            return

        # Write successful event
        data = self._build_payload_event(sip_delivery)

        self.produce_event(
            producer_topic, data, subject, EventOutcome.SUCCESS, event.correlation_id
        )

    def produce_event(
        self,
        topic: str,
        data: dict[str, str],
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

    def receive_message(self) -> None:
        # We use a timeout for PulsarClient.receive because otherwise this is
        # a blocking method that is not guaranteed to return or throw.
        # In order to ensure that the main application loop condition
        # (cf. start_listening) gets checked, we must ensure that receive_message
        # does not block indefinitely.
        try:
            msg = self.pulsar_client.receive(timeout_millis=RECEIVE_MESSAGE_TIMEOUT)
        except Timeout:
            return

        try:
            event = PulsarBinding.from_protocol(msg)  # type: ignore
            self.handle_incoming_message(event)
            self.pulsar_client.acknowledge(msg)
        except Exception as e:
            # Catch and log any errors during message processing
            self.log.error(f"Error: {e}")
            self.pulsar_client.negative_acknowledge(msg)

    def start_listening(self) -> None:
        """
        Starts listening for incoming messages from the Pulsar topic.
        """

        # Add stop handler for SIGTERM and SIGINT signals. This sets
        # self.should_continue to false and ensures that the main loop exits
        # gracefully.
        signal.signal(signal.SIGTERM, self._stop)
        signal.signal(signal.SIGINT, self._stop)

        while self.should_continue:
            self.receive_message()

        self.pulsar_client.close()
