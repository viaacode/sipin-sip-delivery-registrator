import subprocess
from time import sleep
from signal import SIGINT, SIGTERM

import pytest

from app.app import RECEIVE_MESSAGE_TIMEOUT_IN_MS


@pytest.mark.parametrize("stop_signal", [SIGINT, SIGTERM], ids=["sigint", "sigterm"])
def test_graceful_shutdown(
    setup_schema,
    db_client,
    producer,
    insert_sip_delivery,
    outgoing_consumer,
    stop_signal,
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
    process.send_signal(stop_signal)

    # Check that the process ends in the allotted timeout period (+ 1 second
    # to prevent race conditions between timeout and subprocess.)
    try:
        (out, _) = process.communicate(timeout=RECEIVE_MESSAGE_TIMEOUT_IN_MS / 1000 + 1)
    except subprocess.TimeoutExpired:
        process.kill()
        raise Exception("Process did not finish before timeout.")

    # Check for the log message created by the _stop function.
    assert "received a stop signal. Attempting to shut down gracefully." in out

    # Return code should be 0, it should not end due to e.g., an unhandled exception.
    assert process.returncode == 0
