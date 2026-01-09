import os
from uuid import uuid4

import pulsar
from cloudevents.events import CEMessageMode, Event, EventAttributes, PulsarBinding


class ProducerTest:
    """Class to allow producing a s3.object.create event."""

    def __init__(self):
        self.pulsar_host = os.getenv("PULSAR_HOST")
        self.pulsar_port = os.getenv("PULSAR_PORT")
        self.producer_topic = os.getenv("REGISTRATOR_CONSUMER_TOPIC")

        self.client = pulsar.Client(f"pulsar://{self.pulsar_host}:{self.pulsar_port}")
        self.producer = self.client.create_producer(self.producer_topic)

    def produce_event(self, correlation_id: str = str(uuid4())):
        data = {
            "s3_message": {
                "Records": [
                    {
                        "eventVersion": "0.1",
                        "eventSource": "swarm:s3",
                        "eventTime": "2024-07-04 12:41:48,847",
                        "eventName": "ObjectCreated:MULTIPART_COMPLETE",
                        "userIdentity": {"principalId": "aanlevering+or-1111111"},
                        "requestParameters": {"sourceIPAddress": "127.0.0.1"},
                        "responseElements": {
                            "x-request-id": "126E12690C6A2976-06a943108c27f8239d4868cc6ebd5c5f",
                            "x-amz-request-id": "126E12690C6A2976-06a943108c27f8239d4868cc6ebd5c5f",
                        },
                        "s3": {
                            "domain": {
                                "name": "s3.endpoint",
                                "s3-endpoint": "bucketname.s3.endpoint",
                            },
                            "bucket": {
                                "name": "bucketname",
                                "ownerIdentity": {
                                    "principalId": "aanlevering+or-1111111",
                                    "orId": "OR-1111111",
                                },
                            },
                            "object": {"key": "object_key.zip"},
                        },
                    }
                ]
            }
        }

        attr = EventAttributes(correlation_id=correlation_id, subject="subject")
        event = Event(attr, data)
        msg = PulsarBinding.to_protocol(event, CEMessageMode.BINARY)
        self.producer.send(
            msg.data,
            properties=msg.attributes,
            event_timestamp=event.get_event_time_as_int(),
        )
