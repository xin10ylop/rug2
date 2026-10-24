"""Yellowstone (Dragon's Mouth) gRPC client: Subscribe + SubscribeDeshred with
dynamic filter updates, keepalive pings and automatic reconnect.

Stubs are generated from rugsnipe/proto/*.proto by `scripts/gen_protos.sh`
(or on first import if grpc_tools is installed).
"""
from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import sys
import time
from typing import AsyncIterator, Awaitable, Callable, Optional

import grpc

log = logging.getLogger("grpc")

_PROTO_DIR = os.path.join(os.path.dirname(__file__), "proto")


def _ensure_stubs():
    if not os.path.exists(os.path.join(_PROTO_DIR, "geyser_pb2.py")):
        subprocess.check_call([sys.executable, "-m", "grpc_tools.protoc", f"-I{_PROTO_DIR}",
                               f"--python_out={_PROTO_DIR}", f"--grpc_python_out={_PROTO_DIR}",
                               os.path.join(_PROTO_DIR, "geyser.proto"),
                               os.path.join(_PROTO_DIR, "solana-storage.proto")])
    if _PROTO_DIR not in sys.path:
        sys.path.insert(0, _PROTO_DIR)


_ensure_stubs()
import geyser_pb2 as G  # noqa: E402
import geyser_pb2_grpc as GG  # noqa: E402

PROCESSED = G.CommitmentLevel.PROCESSED
CONFIRMED = G.CommitmentLevel.CONFIRMED


def make_channel(endpoint: str, x_token: Optional[str]) -> grpc.aio.Channel:
    target = endpoint.replace("https://", "").replace("http://", "")
    if ":" not in target.split("/")[0]:
        target = target.split("/")[0] + ":443"
    opts = [
        ("grpc.max_receive_message_length", 128 * 1024 * 1024),
        ("grpc.keepalive_time_ms", 10_000),
        ("grpc.keepalive_timeout_ms", 5_000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
        ("grpc.enable_retries", 0),
    ]
    creds = grpc.ssl_channel_credentials()
    if x_token:
        auth = grpc.metadata_call_credentials(lambda ctx, cb: cb((("x-token", x_token),), None))
        creds = grpc.composite_channel_credentials(creds, auth)
    return grpc.aio.secure_channel(target, creds, options=opts)


class Subscription:
    """A long-lived bidirectional stream whose filter can be replaced at any time.

    build_request() is called on every (re)connect and on update(); it must return
    a complete SubscribeRequest / SubscribeDeshredRequest for the current state.
    """

    def __init__(self, name: str, endpoint: str, x_token: Optional[str],
                 build_request: Callable[[], object], on_update: Callable[[object], Awaitable[None] | None],
                 deshred: bool = False, ping_every: float = 15.0):
        self.name = name
        self.endpoint = endpoint
        self.x_token = x_token
        self.build_request = build_request
        self.on_update = on_update
        self.deshred = deshred
        self.ping_every = ping_every
        self._q: asyncio.Queue = asyncio.Queue()
        self._stop = asyncio.Event()
        self.connected = False
        self.last_msg = 0.0
        self.n_msgs = 0
        self.reconnects = 0

    def update(self):
        """Push the current filter set to the server (no reconnect)."""
        self._q.put_nowait(self.build_request())

    def stop(self):
        self._stop.set()

    async def _requests(self) -> AsyncIterator:
        yield self.build_request()
        ping_id = 0
        while not self._stop.is_set():
            try:
                req = await asyncio.wait_for(self._q.get(), timeout=self.ping_every)
                yield req
            except asyncio.TimeoutError:
                ping_id += 1
                if self.deshred:
                    yield G.SubscribeDeshredRequest(ping=G.SubscribeRequestPing(id=ping_id))
                else:
                    yield G.SubscribeRequest(ping=G.SubscribeRequestPing(id=ping_id))

    async def run(self):
        backoff = 0.5
        while not self._stop.is_set():
            ch = make_channel(self.endpoint, self.x_token)
            try:
                stub = GG.GeyserStub(ch)
                # drain stale queued updates; the first request carries the full current filter
                while not self._q.empty():
                    self._q.get_nowait()
                call = stub.SubscribeDeshred(self._requests()) if self.deshred else stub.Subscribe(self._requests())
                self.connected = True
                log.info("[%s] connected", self.name)
                backoff = 0.5
                async for upd in call:
                    self.last_msg = time.time()
                    self.n_msgs += 1
                    r = self.on_update(upd)
                    if asyncio.iscoroutine(r):
                        await r
                    if self._stop.is_set():
                        call.cancel()
                        break
            except grpc.aio.AioRpcError as e:
                log.warning("[%s] stream error %s %s", self.name, e.code(), (e.details() or "")[:200])
            except Exception:
                log.exception("[%s] stream crashed", self.name)
            finally:
                self.connected = False
                await ch.close()
            if self._stop.is_set():
                break
            self.reconnects += 1
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)


async def latest_blockhash(endpoint: str, x_token: Optional[str]) -> tuple[str, int]:
    async with make_channel(endpoint, x_token) as ch:
        r = await GG.GeyserStub(ch).GetLatestBlockhash(G.GetLatestBlockhashRequest(commitment=PROCESSED))
        return r.blockhash, r.last_valid_block_height
