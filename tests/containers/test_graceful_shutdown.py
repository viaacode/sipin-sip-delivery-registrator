import subprocess
from pathlib import Path
from time import sleep
from signal import SIGINT, SIGTERM

from app.app import RECEIVE_MESSAGE_TIMEOUT_IN_MS

def test_graceful_shutdown_sigint(
    setup_schema,
    db_client,
    producer,
    insert_sip_delivery,
    outgoing_consumer,
):
    process = subprocess.Popen(
        ["python", "-m", "main"],
        encoding="utf-8",
        stdout=subprocess.PIPE,
    )

    # Wait for a small amount of time, as there is probably a race condition between
    # this process calling `process.send_signal` and the subprocess registering
    # signal handlers.
    sleep(1)
    process.send_signal(SIGINT)

    # Again, probably a race condition between the `process.poll()` and the process
    # actually ending in case graceful shutdown fails.
    sleep(1)
    exited = process.poll()
    assert exited is None

    # After waiting for ~RECEIVE_MESSAGE_TIMEOUT_IN_MS milliseconds without any
    # messages coming in, the process should have finished due to timeout.
    sleep(RECEIVE_MESSAGE_TIMEOUT_IN_MS / 1000)
    exited = process.poll()
    assert exited == 0

def test_graceful_shutdown_sigterm(
    setup_schema,
    db_client,
    producer,
    insert_sip_delivery,
    outgoing_consumer,
):
    process = subprocess.Popen(
        ["python", "-m", "main"],
        encoding="utf-8",
        stdout=subprocess.PIPE,
    )

    # Wait for a small amount of time, as there is probably a race condition between
    # this process calling `process.send_signal` and the subprocess registering
    # signal handlers.
    sleep(1)
    process.send_signal(SIGTERM)

    # Again, probably a race condition between the `process.poll()` and the process
    # actually ending in case graceful shutdown fails.
    sleep(1)
    exited = process.poll()
    assert exited is None

    # After waiting for ~RECEIVE_MESSAGE_TIMEOUT_IN_MS milliseconds without any
    # messages coming in, the process should have finished due to timeout.
    sleep(RECEIVE_MESSAGE_TIMEOUT_IN_MS / 1000)
    exited = process.poll()
    assert exited == 0