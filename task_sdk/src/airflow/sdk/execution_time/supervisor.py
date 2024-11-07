#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Supervise and run Tasks in a subprocess."""

from __future__ import annotations

import atexit
import io
import logging
import os
import selectors
import signal
import sys
import time
import weakref
from collections.abc import Generator
from contextlib import suppress
from datetime import datetime
from socket import socket, socketpair
from typing import TYPE_CHECKING, Any, BinaryIO, Callable, ClassVar, Literal, NoReturn, cast, overload

import attrs
import msgspec
import psutil
import structlog

from airflow.sdk.execution_time.comms import StartupDetails, ToSupervisor

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.execution_time.comms import ExecuteTaskActivity


__all__ = ["WatchedSubprocess"]

log: FilteringBoundLogger = structlog.get_logger(logger_name="supervisor")


@overload
def mkpipe() -> tuple[socket, socket]: ...


@overload
def mkpipe(remote_read: Literal[True]) -> tuple[socket, BinaryIO]: ...


def mkpipe(
    remote_read: bool = False,
) -> tuple[socket, socket | BinaryIO]:
    """
    Create a pair of connected sockets.

    The inheritable flag will be set correctly so that the end destined for the subprocess is kept open but
    the end for this process is closed automatically by the OS.
    """
    rsock, wsock = socketpair()
    local, remote = (wsock, rsock) if remote_read else (rsock, wsock)

    remote.set_inheritable(True)
    local.setblocking(False)

    io: BinaryIO | socket
    if remote_read:
        # If _we_ are writing, we don't want to buffer
        io = cast(BinaryIO, local.makefile("wb", buffering=0))
    else:
        io = local

    return remote, io


def _subprocess_main():
    from airflow.sdk.execution_time.task_runner import main

    main()


def _fork_main(
    child_stdin: socket,
    child_stdout: socket,
    child_stderr: socket,
    log_fd: int,
    target: Callable[[], None],
) -> NoReturn:
    # Uninstall the rich etc. exception handler
    sys.excepthook = sys.__excepthook__
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGUSR2, signal.SIG_DFL)

    if log_fd > 0:
        # A channel that the task can send JSON-formated logs over.
        #
        # JSON logs sent this way will be handled nicely
        from airflow.sdk.log import configure_logging

        log_io = os.fdopen(log_fd, "wb", buffering=0)
        configure_logging(enable_pretty_log=False, output=log_io)

    last_chance_stderr = sys.__stderr__ or sys.stderr

    # Ensure that sys.stdout et al (and the underlying filehandles for C libraries etc) are connected to the
    # pipes form the supervisor

    for handle_name, sock, mode, close in (
        ("stdin", child_stdin, "r", True),
        ("stdout", child_stdout, "w", True),
        ("stderr", child_stderr, "w", False),
    ):
        handle = getattr(sys, handle_name)
        try:
            fd = handle.fileno()
            os.dup2(sock.fileno(), fd)
            if close:
                handle.close()
        except io.UnsupportedOperation:
            if "PYTEST_CURRENT_TEST" in os.environ:
                # When we're running under pytest, the stdin is not a real filehandle with an fd, so we need
                # to handle that differently
                fd = sock.fileno()
            else:
                raise

        setattr(sys, handle_name, os.fdopen(fd, mode))

    def exit(n: int) -> NoReturn:
        with suppress(ValueError):
            sys.stdout.flush()
        with suppress(ValueError):
            sys.stderr.flush()
        with suppress(ValueError):
            last_chance_stderr.flush()
        os._exit(n)

    if hasattr(atexit, "_clear"):
        # Since we're in a fork we want to try and clear them
        atexit._clear()
        base_exit = exit

        def exit(n: int) -> NoReturn:
            atexit._run_exitfuncs()
            base_exit(n)

    try:
        target()
        exit(0)
    except SystemExit as e:
        code = 1
        if isinstance(e.code, int):
            code = e.code
        elif e.code:
            print(e.code, file=sys.stderr)
        exit(code)
    except Exception:
        # Last ditch log attempt
        exc, v, tb = sys.exc_info()

        import traceback

        try:
            last_chance_stderr.write("--- Last chance exception handler ---\n")
            traceback.print_exception(exc, value=v, tb=tb, file=last_chance_stderr)
            exit(99)
        except Exception as e:
            with suppress(Exception):
                print(
                    f"--- Last chance exception handler failed --- {repr(str(e))}\n", file=last_chance_stderr
                )
            exit(98)


@attrs.define()
class WatchedSubprocess:
    pid: int

    stdin: BinaryIO
    stdout: socket
    stderr: socket

    _process: psutil.Process
    _exit_code: int | None = None
    _terminal_state: str | None = None

    selector: selectors.BaseSelector = attrs.field(factory=selectors.DefaultSelector)

    procs: ClassVar[weakref.WeakValueDictionary[int, WatchedSubprocess]] = weakref.WeakValueDictionary()

    def __attrs_post_init__(self):
        self.procs[self.pid] = self

    @classmethod
    def start(
        cls, path: str | os.PathLike[str], ti: Any, target: Callable[[], None] = _subprocess_main
    ) -> WatchedSubprocess:
        """Fork and start a new subprocess to execute the given task."""
        # Create socketpairs/"pipes" to connect to the stdin and out from the subprocess
        child_stdin, feed_stdin = mkpipe(remote_read=True)
        child_stdout, read_stdout = mkpipe()
        child_stderr, read_stderr = mkpipe()

        # Open these socketpair before forking off the child, so that it is open when we fork.
        child_comms, read_msgs = mkpipe()
        child_logs, read_logs = mkpipe()

        pid = os.fork()
        if pid == 0:
            # Parent ends of the sockets are closed by the OS as they are set as non-inheritable

            # Run the child entryoint
            _fork_main(child_stdin, child_stdout, child_stderr, child_logs.fileno(), target)

        proc = cls(
            pid=pid,
            stdin=feed_stdin,
            stdout=read_stdout,
            stderr=read_stderr,
            process=psutil.Process(pid),
        )

        # TODO: Use logging providers to handle the chunked upload for us
        task_logger: FilteringBoundLogger = structlog.get_logger(logger_name="task").bind()

        cb = make_buffered_socket_reader(forward_to_log(task_logger.bind(chan="stdout"), level=logging.INFO))

        proc.selector.register(read_stdout, selectors.EVENT_READ, cb)
        cb = make_buffered_socket_reader(forward_to_log(task_logger.bind(chan="stderr"), level=logging.ERROR))
        proc.selector.register(read_stderr, selectors.EVENT_READ, cb)

        proc.selector.register(
            read_logs,
            selectors.EVENT_READ,
            make_buffered_socket_reader(process_log_messages_from_subprocess(task_logger)),
        )
        proc.selector.register(
            read_msgs,
            selectors.EVENT_READ,
            make_buffered_socket_reader(proc.handle_requests(log=log)),
        )

        # Tell the task process what it needs to do!
        msg = StartupDetails(
            ti=ti,
            file=str(path),
            requests_fd=child_comms.fileno(),
        )

        # Close the remaining parent-end of the sockets we've passed to the child via fork. We still have the
        # other end of the pair open
        child_stdout.close()
        child_stdin.close()
        child_comms.close()
        child_logs.close()

        # Send the message to tell the process what it needs to execute
        log.debug("Sending", msg=msg)
        feed_stdin.write(msgspec.json.encode(msg))
        feed_stdin.write(b"\n")
        # feed_stdin.flush()

        return proc

    def kill(self, signal: signal.Signals = signal.SIGINT):
        if self._exit_code is not None:
            return

        with suppress(ProcessLookupError):
            os.kill(self.pid, signal)

    def wait(self) -> int:
        if self._exit_code is not None:
            return self._exit_code

        try:
            while self._exit_code is None or len(self.selector.get_map()):
                events = self.selector.select(timeout=10.0)
                for key, _ in events:
                    callback = key.data
                    open = callback(key.fileobj)

                    if not open:
                        log.debug("Remote end closed, closing", fileobj=key.fileobj)
                        self.selector.unregister(key.fileobj)
                        key.fileobj.close()  # type: ignore[union-attr]
                # TODO: Send heartbeat here
                try:
                    self._exit_code = self._process.wait(timeout=0.1)
                except psutil.TimeoutExpired:
                    pass
        finally:
            self.selector.close()
        return self._exit_code

    @property
    def final_state(self):
        """
        The final state of the TaskInstance.

        By default this will be derived from the exit code of the task
        (0=success, failed otherwise) but can be changed by the subprocess
        sending a TaskState message, as long as the process exits with 0

        Not valid before the process has finished.
        """
        # TODO: state enums
        if self._exit_code == 0:
            return self._terminal_state if self._terminal_state is not None else "success"
        return "failed"

    def __rich_repr__(self):
        yield "pid", self.pid
        yield "exit_code", self._exit_code, None

    __rich_repr__.angular = True  # type: ignore[attr-defined]

    def __repr__(self) -> str:
        rep = f"<WatchedSubprocess pid={self.pid}"
        if self._exit_code is not None:
            rep += f" exit_code={self._exit_code}"
        return rep + " >"

    def handle_requests(self, log: FilteringBoundLogger) -> Generator[None, bytes, None]:
        decoder: msgspec.json.Decoder[ToSupervisor] = msgspec.json.Decoder(type=ToSupervisor)
        # encoder = msgspec.json.Encoder()
        # buffer = bytearray(64)
        while True:
            line = yield

            try:
                msg = decoder.decode(line)
            except Exception:
                log.exception("Unable to decode message", line=line)
                continue

            # if isinstnace(msg, TaskState):
            #     self._terminal_state = msg.state
            # elif isinstance(msg, ReadXCom):
            #     resp = XComResponse(key="secret", value=True)
            #     encoder.encode_into(resp, buffer)
            #     self.stdin.write(buffer + b"\n")
            # elif isinstance(msg, GetConnection):
            #     resp = ConnectionResponse(
            #         Connection(
            #             conn_id=msg.id,
            #             conn_type="sqlite",
            #             host="test-db.sqlite",
            #         )
            #     )
            #     encoder.encode_into(resp, buffer)
            #     self.stdin.write(buffer + b"\n")
            # else:
            log.error("Unhandled request", msg=msg)


# Sockets, even the `.makefile()` function don't correctly do line buffering on reading. If a chunk is read
# and it doesn't contain a new line character, `.readline()` will just return the chunk as is.
#
# This returns a cb suitable for attaching to a `selector` that reads in to a buffer, and yields lines to a
# (sync) generator
def make_buffered_socket_reader(
    gen: Generator[None, bytes, None], buffer_size: int = 4096
) -> Callable[[socket], bool]:
    buffer = bytearray()  # This will hold our accumulated binary data
    read_buffer = bytearray(buffer_size)  # Temporary buffer for each read

    # We need to start up the generator to get it to the point it's at waiting on the yield
    next(gen)

    def cb(sock: socket):
        nonlocal buffer, read_buffer
        # Read up to `buffer_size` bytes of data from the socket
        n_received = sock.recv_into(read_buffer)

        if not n_received:
            # If no data is returned, the connection is closed. Return whatever is left in the buffer
            if len(buffer):
                gen.send(buffer)
            # Tell loop to close this selector
            return False

        buffer.extend(read_buffer[:n_received])

        # We could have read multiple lines in one go, yield them all
        while (newline_pos := buffer.find(b"\n")) != -1:
            if TYPE_CHECKING:
                # We send in a memoryvuew, but pretend it's a bytes, as Buffer is only in 3.12+
                line = buffer[: newline_pos + 1]
            else:
                line = memoryview(buffer)[: newline_pos + 1]  # Include the newline character
            gen.send(line)
            buffer = buffer[newline_pos + 1 :]  # Update the buffer with remaining data

        return True

    return cb


def process_log_messages_from_subprocess(log: FilteringBoundLogger) -> Generator[None, bytes, None]:
    from structlog.stdlib import NAME_TO_LEVEL

    while True:
        # Generator receive syntax, values are "sent" in  by the `make_buffered_socket_reader` and returned to
        # the yield.
        line = yield

        try:
            event = msgspec.json.decode(line)
        except Exception:
            log.exception("Malformed json log line", line=line)
            continue

        if ts := event.get("timestamp"):
            # We use msgspec to decode the json as it does it orders of magnitude quicker than
            # datetime.strptime does
            # TODO: don't hard-code the time format here
            event["timestamp"] = msgspec.json.decode(f'"{ts}"', type=datetime)

        if exc := event.pop("exception", None):
            # TODO: convert the dict back to a pretty stack trace
            event["error_detail"] = exc
        log.log(NAME_TO_LEVEL[event.pop("level")], event.pop("event", None), **event)

    log.debug("stream closed")


def forward_to_log(target_log: FilteringBoundLogger, level: int) -> Generator[None, bytes, None]:
    while True:
        buf = yield
        line = bytes(buf)
        # Strip off new line
        line = line.rstrip()
        try:
            msg = line.decode("utf-8", errors="replace")
            target_log.log(level, msg)
        except UnicodeDecodeError:
            msg = line.decode("ascii", errors="replace")
            target_log.log(level, msg)


def supervise(activity: ExecuteTaskActivity, server: str | None = None, dry_run: bool = False) -> int:
    """
    Run a single task execution to completion.

    Returns the exit code of the process
    """
    # One or the other
    if (server == "") ^ dry_run:
        raise ValueError(f"Can only specify one of {server=} or {dry_run=}")

    if not activity.path:
        raise ValueError("path filed of activity missing")

    start = time.monotonic()

    process = WatchedSubprocess.start(activity.path, activity.ti)

    exit_code = process.wait()
    end = time.monotonic()
    log.debug("Process exited", exit_code=exit_code, duration=end - start)
    return exit_code
