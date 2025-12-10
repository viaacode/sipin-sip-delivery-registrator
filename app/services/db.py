# Standard
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from psycopg.errors import UniqueViolation
from psycopg_pool import ConnectionPool
from viaa.configuration import ConfigParser
from viaa.observability import logging


class SipStatus(StrEnum):
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILURE = "failure"


@dataclass
class SipDelivery:
    correlation_id: str
    s3_bucket: str
    s3_object_key: str
    s3_domain: str
    pid: str | None = field(default=None)
    status: SipStatus = field(default=SipStatus.IN_PROGRESS)
    failure_message: str | None = field(default=None)
    last_event_type: str = field(default="sipin/sip.registered")
    last_event_occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class DuplicateKeyError(Exception):
    """Error when inserting with a duplicate key (correlation_id)

    This is an abstraction of Psycopg's UniqueViolation.
    """

    pass


class DbClient:
    def __init__(self, config_parser: ConfigParser):
        self.log = logging.get_logger(__name__, config=config_parser)
        self.db_config: dict = config_parser.app_cfg["db"]
        self.pool = ConnectionPool(
            f"host={self.db_config['host']} port={self.db_config['port']} dbname={self.db_config['dbname']} user={self.db_config['username']} password={self.db_config['password']}",
            open=True,
        )
        self.table = self.db_config["table"]

    def insert_sip_delivery(self, sip_delivery: SipDelivery):
        """Insert a delivery of a SIP into the database.

        Args:
            sip_delivery: A delivered SIP.

        Raises:
            DuplicateKeyError: If the record cannot be inserted because of duplicate correlation_id.
        """
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"INSERT INTO public.{self.table} (correlation_id, s3_bucket, s3_object_key, last_event_type, last_event_occurred_at) VALUES (%s, %s, %s, %s, %s);",
                        (
                            sip_delivery.correlation_id,
                            sip_delivery.s3_bucket,
                            sip_delivery.s3_object_key,
                            sip_delivery.last_event_type,
                            sip_delivery.last_event_occurred_at,
                        ),
                    )
                    conn.commit()
        except UniqueViolation as e:
            raise DuplicateKeyError(str(e)) from e

    def close(self):
        """Close the connection (pool)"""
        self.pool.close()
