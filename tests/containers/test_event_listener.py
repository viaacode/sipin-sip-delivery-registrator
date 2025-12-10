from datetime import UTC, datetime
from uuid import uuid4

from cloudevents.events import PulsarBinding


def test_receive_message(
    setup_schema, db_client, event_listener, producer, outgoing_consumer
):
    """
    Normal flow:
      - Consume a message
      - Persist a record
      - Produce an event
      - Assert said event
    """
    correlation_id = str(uuid4())

    # Produce test event and run the listener
    producer.produce_event(correlation_id)
    event_listener.receive_message()

    # Assert DB state
    with db_client.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT correlation_id, s3_bucket, s3_object_key, pid, status, failure_message, last_event_type, last_event_occurred_at, \
                 created_at, updated_at FROM public.sip_deliveries WHERE correlation_id = %s",
                (correlation_id,),
            )
            (
                correlation_id,
                bucket,
                key,
                pid,
                status,
                failure_message,
                last_event_type,
                last_event_occurred_at,
                created_at,
                updated_at,
            ) = cur.fetchone()

    # Assert new record
    assert correlation_id == correlation_id
    assert bucket == "bucketname"
    assert key == "object_key.zip"
    assert pid is None
    assert status == "in_progress"
    assert failure_message is None
    assert last_event_type == "sipin/sip.registered"
    assert last_event_occurred_at < datetime.now(UTC)
    assert created_at < datetime.now(UTC)
    assert updated_at < datetime.now(UTC)

    # Assert produced event
    msg = outgoing_consumer.receive(timeout_millis=5000)
    outgoing_consumer.acknowledge(msg)

    event = PulsarBinding.from_protocol(msg)
    properties = event.get_attributes()
    assert properties.get("correlation_id") == correlation_id
    assert event.has_successful_outcome()
    assert properties.get("subject") == "subject"

    payload = event.get_data()
    assert payload["s3_bucket"] == "bucketname"
    assert payload["s3_object_key"] == "object_key.zip"
    assert payload["s3_domain"] == "s3.endpoint"
    assert "message" not in payload


def test_receive_message_duplicate_key(
    setup_schema,
    db_client,
    event_listener,
    producer,
    insert_sip_delivery,
    outgoing_consumer,
):
    """
    Flow duplicate key:
      - Pre-insert the record as test setup
      - Send the same message
      - Verify that the record still exists with expected values
      - Produce failed event
      - Assert said event
    """
    correlation_id = str(uuid4())

    # Insert existing record in DB
    insert_sip_delivery(correlation_id)

    # Produce test event and run the listener
    producer.produce_event(correlation_id)
    event_listener.receive_message()

    # Check only one record in database
    with db_client.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM public.sip_deliveries WHERE correlation_id = %s",
                (correlation_id,),
            )
            (count,) = cur.fetchone()
            assert count == 1

            cur.execute(
                "SELECT s3_bucket, s3_object_key, status, failure_message, last_event_type FROM public.sip_deliveries WHERE correlation_id = %s",
                (correlation_id,),
            )
            s3_bucket, s3_object_key, status, failure_message, last_event_type = (
                cur.fetchone()
            )

    # Assert existing record
    assert s3_bucket == "bucketname"
    assert s3_object_key == "object_key.zip"
    assert status == "in_progress"
    assert failure_message is None
    assert last_event_type == "sipin/sip.registered"

    # Assert produced event
    msg = outgoing_consumer.receive(timeout_millis=5000)
    outgoing_consumer.acknowledge(msg)

    event = PulsarBinding.from_protocol(msg)
    properties = event.get_attributes()
    assert properties.get("correlation_id") == correlation_id
    assert not event.has_successful_outcome()
    assert properties.get("subject") == "subject"

    payload = event.get_data()
    assert payload["s3_bucket"] == "bucketname"
    assert payload["s3_object_key"] == "object_key.zip"
    assert payload["s3_domain"] == "s3.endpoint"
    assert "message" in payload
