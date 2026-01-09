import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pulsar
import pytest
import requests
from producer import ProducerTest
from testcontainers.core.generic import DockerContainer
from testcontainers.postgres import PostgresContainer
from viaa.configuration import ConfigParser

from app.app import EventListener
from app.services.db import DbClient

PRODUCER_TOPIC = "persistent://public/default/outgoing"
CONSUMER_TOPIC = "persistent://public/default/incoming"


# Database
@pytest.fixture(scope="function")
def postgres_env(monkeypatch):
    """Spins up Postgres and sets env vars expected by DbClient."""
    with PostgresContainer("postgres:16") as pg:
        monkeypatch.setenv("DB_HOST", pg.get_container_host_ip())
        monkeypatch.setenv("DB_PORT", str(pg.get_exposed_port(5432)))
        monkeypatch.setenv("DB_USERNAME", pg.username)
        monkeypatch.setenv("DB_PASSWORD", pg.password)
        monkeypatch.setenv("DB_NAME", pg.dbname)
        monkeypatch.setenv("DB_TABLE", "sip_deliveries")

        yield {
            "host": pg.get_container_host_ip(),
            "port": str(pg.get_exposed_port(5432)),
            "dbname": pg.dbname,
            "username": pg.username,
            "password": pg.password,
            "table": "sip_deliveries",
        }


@pytest.fixture(scope="function")
def db_client(config_parser):
    """Returns a DB client."""
    return DbClient(config_parser)


@pytest.fixture(scope="function")
def setup_schema(db_client):
    """Creates enum, table and trigger."""
    with open(
        Path("..", "sipin_postgres", "sip_deliveries.ddl"), "r", encoding="utf-8"
    ) as ddl_file:
        ddl = ddl_file.read()
    with db_client.pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


@pytest.fixture(scope="function")
def insert_sip_delivery(db_client):
    """Returns a callable that inserts a record into the database."""

    def _insert(
        correlation_id: str,
        s3_bucket: str = "bucketname",
        s3_object_key: str = "object_key.zip",
        pid: str | None = None,
        status: str = "in_progress",
        failure_message: str | None = None,
        last_event_type: str = "sipin/sip.registered",
        last_event_occurred_at: datetime | None = None,
    ):
        ts = last_event_occurred_at or datetime.now(UTC)
        with db_client.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO public.sip_deliveries (
                      correlation_id, s3_bucket, s3_object_key, pid,
                      status, failure_message, last_event_type,
                      last_event_occurred_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        correlation_id,
                        s3_bucket,
                        s3_object_key,
                        pid,
                        status,
                        failure_message,
                        last_event_type,
                        ts,
                    ),
                )
        return correlation_id

    return _insert


# Pulsar
def _wait_for_pulsar(host: str, admin_port: int, timeout: int = 90, min_sleep: int = 10):
    """Waits until Pulsar is healthy."""
    url = f"http://{host}:{admin_port}/admin/v2/brokers/health"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=2)
            if r.status_code == 200:
                time.sleep(min_sleep)  # To ensure it is actually ready
                return
        except Exception:
            pass
        time.sleep(1)
    raise TimeoutError(f"Pulsar did not become healthy on {url} in {timeout}s")


@pytest.fixture(scope="function")
def pulsar_env(monkeypatch):
    """Spins up Apache Pulsar (standalone) and sets env vars expected by PulsarClient."""
    with (
        DockerContainer("apachepulsar/pulsar:latest")
        .with_command("/pulsar/bin/pulsar standalone")
        .with_exposed_ports(6650, 8080)
    ) as pulsar_ct:
        host = pulsar_ct.get_container_host_ip()
        pulsar_port = pulsar_ct.get_exposed_port(6650)
        admin_port = pulsar_ct.get_exposed_port(8080)

        _wait_for_pulsar(host, admin_port)

        # Set env vars to use the values exposed in the container
        monkeypatch.setenv("PULSAR_HOST", host)
        monkeypatch.setenv("PULSAR_PORT", str(pulsar_port))

        monkeypatch.setenv("REGISTRATOR_CONSUMER_TOPIC", CONSUMER_TOPIC)
        monkeypatch.setenv("REGISTRATOR_PRODUCER_TOPIC", PRODUCER_TOPIC)

        yield {
            "host": host,
            "port": str(pulsar_port),
            "PRODUCER_TOPIC": PRODUCER_TOPIC,
            "CONSUMER_TOPIC": CONSUMER_TOPIC,
        }


@pytest.fixture(scope="function")
def outgoing_consumer(pulsar_env):
    """Creates a consumer for the outgoing topic.

    Different subscription per run to guarantee no cross-talk.
    """
    client = pulsar.Client(f"pulsar://{pulsar_env['host']}:{pulsar_env['port']}")
    sub = f"test-outgoing-{uuid.uuid4()}"
    consumer = client.subscribe(PRODUCER_TOPIC, subscription_name=sub)

    try:
        yield consumer
    finally:
        try:
            consumer.close()
        finally:
            client.close()


@pytest.fixture(scope="function")
def producer():
    """Fixture for ProducerTest."""
    return ProducerTest()


# Config
@pytest.fixture(scope="function")
def config_parser(pulsar_env, postgres_env):
    """Fixture for ConfigParser."""
    return ConfigParser()


# Application
@pytest.fixture(scope="function")
def event_listener(config_parser):
    """Fixture for EventListener."""
    return EventListener()
