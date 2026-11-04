from __future__ import annotations

import asyncio
import json
import os
import ssl
import stat
import subprocess
import tempfile
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import pytest
from aiohttp import web
from livekit.agents import APIConnectionError, APIConnectOptions
from livekit.agents import llm as agents_llm

from speko_gateway import GatewayError, RelayLLMClient, RelaySpeechClient
from speko_gateway._livekit_bridge import LiveKitSTTBridge, LiveKitTTSBridge
from speko_gateway._relay_socket import NATIVE_LOCAL_AUTHORIZATION
from speko_gateway.livekit import LLM
from speko_gateway.relay import RelayError

LOCAL_TOKEN = "owned-native-local-token"
MODEL_KEY = "sk_speko_agent_" + "a" * 64


def clean_environment():
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("SPEKO_", "AWS_"))
        and name not in {"SSL_CERT_FILE", "SSL_CERT_DIR"}
    }
    env.update(GOWORK="off", GOTOOLCHAIN="local")
    return env


@pytest.fixture(scope="session")
def native_binary(tmp_path_factory, pytestconfig):
    directory = tmp_path_factory.mktemp("native-gateway-build")
    binary = directory / "speko-gateway"
    image = pytestconfig.getoption("native_gateway_image")
    if image is None:
        subprocess.run(
            ["go", "build", "-o", str(binary), "./cmd/speko-gateway"],
            cwd=Path(__file__).resolve().parents[3],
            env=clean_environment(),
            check=True,
            capture_output=True,
        )
    certificate, key = directory / "peer.pem", directory / "peer-key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(certificate),
            "-subj",
            "/CN=owned-native-peer",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-days",
            "1",
        ],
        check=True,
        capture_output=True,
    )
    key.chmod(0o600)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    return binary, certificate, context, image


@asynccontextmanager
async def native_stack(native_binary, handler, *, crash_once=False):
    binary, certificate, tls, image = native_binary
    app = web.Application()
    app.router.add_route("*", "/{path:.*}", handler)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    with tempfile.TemporaryDirectory(prefix="native-sidecar-", dir="/tmp") as directory:
        socket_path = str(Path(directory) / "runtime.sock")
        process = output = None
        container_name = None
        prior_logs = []
        try:
            await web.TCPSite(runner, "127.0.0.1", 0, ssl_context=tls).start()
            origin = f"https://127.0.0.1:{runner.addresses[0][1]}"
            env = clean_environment()
            env.update(
                SPEKO_RELAY_URL=origin,
                SPEKO_SOCKET_PATH=socket_path,
                SPEKO_LOCAL_AUTH_TOKEN=LOCAL_TOKEN,
                SSL_CERT_FILE=str(certificate),
                SPEKO_API_KEY_FILE="/must-not-read-a-global-model-key",
                SPEKO_CARTESIA_BYOK_API_KEY_FILE="/must-not-read-provider-credentials",
            )
            command = [str(binary), "native"]
            if image is not None:
                assert os.getuid() != 0, "The image smoke must exercise a non-root UID"
                container_name = "native-sidecar-test-" + uuid.uuid4().hex
                command = [
                    "docker",
                    "run",
                    "--rm",
                    "--name",
                    container_name,
                    "--network",
                    "host",
                    "--read-only",
                    "--cap-drop",
                    "ALL",
                    "--security-opt",
                    "no-new-privileges:true",
                    "--user",
                    f"{os.getuid()}:{os.getgid()}",
                    "--mount",
                    f"type=bind,source={directory},target=/run/speko",
                    "--mount",
                    f"type=bind,source={certificate},target=/run/owned-peer.pem,readonly",
                    "--env",
                    f"SPEKO_RELAY_URL={origin}",
                    "--env",
                    f"SPEKO_LOCAL_AUTH_TOKEN={LOCAL_TOKEN}",
                    "--env",
                    "SPEKO_SOCKET_PATH=/run/speko/runtime.sock",
                    "--env",
                    "SSL_CERT_FILE=/run/owned-peer.pem",
                    "--env",
                    "SPEKO_API_KEY_FILE=/must-not-read-a-global-model-key",
                    "--env",
                    "SPEKO_CARTESIA_BYOK_API_KEY_FILE=/must-not-read-provider-credentials",
                    image,
                    "native",
                ]
            for boot in range(2 if crash_once else 1):
                process = await asyncio.create_subprocess_exec(
                    *command,
                    env=env,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
                output = asyncio.create_task(process.stdout.read())
                async with aiohttp.ClientSession(
                    connector=aiohttp.UnixConnector(path=socket_path)
                ) as probe:
                    deadline = asyncio.get_running_loop().time() + 5
                    while True:
                        if process.returncode is not None:
                            raise RuntimeError(
                                "Native Gateway command exited during startup"
                            )
                        try:
                            async with probe.get(
                                "http://speko-gateway/readyz"
                            ) as response:
                                if response.status == 200:
                                    break
                        except aiohttp.ClientError:
                            pass
                        if asyncio.get_running_loop().time() > deadline:
                            raise TimeoutError("Native Gateway did not start")
                        await asyncio.sleep(0.01)
                if crash_once and boot == 0:
                    if container_name is None:
                        process.kill()
                    else:
                        kill = await asyncio.create_subprocess_exec(
                            "docker",
                            "kill",
                            "--signal",
                            "KILL",
                            container_name,
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.DEVNULL,
                        )
                        assert await kill.wait() == 0
                    assert await asyncio.wait_for(process.wait(), 5) != 0
                    prior_logs.append(await output)
                    assert Path(socket_path).is_socket(), (
                        "SIGKILL must leave the stale socket"
                    )
                    if container_name is not None:
                        remove = await asyncio.create_subprocess_exec(
                            "docker",
                            "rm",
                            "--force",
                            container_name,
                            stdout=asyncio.subprocess.DEVNULL,
                            stderr=asyncio.subprocess.DEVNULL,
                        )
                        await remove.wait()
            assert stat.S_IMODE(Path(socket_path).stat().st_mode) == 0o600
            yield socket_path, process
        finally:
            if process is not None:
                if process.returncode is None:
                    process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 5)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
                logs = b"".join([*prior_logs, await output])
                if container_name is not None:
                    remove = await asyncio.create_subprocess_exec(
                        "docker",
                        "rm",
                        "--force",
                        container_name,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                    )
                    await remove.wait()
                assert MODEL_KEY.encode() not in logs
                assert LOCAL_TOKEN.encode() not in logs
                assert b"owned-private-prompt" not in logs
            await runner.cleanup()


async def ready(ws):
    await ws.send_json(
        {
            "type": "session.ready",
            "request_id": "native-peer",
            "route": {
                "provider": "fixture",
                "model": "fixture-model",
                "region": "owned",
            },
        }
    )


@pytest.mark.parametrize("large", ["prefix", "inference"])
async def test_actual_native_sidecar_accepts_large_livekit_vad_audio(
    native_binary, large
):
    from livekit import rtc
    from livekit.agents import stt, vad
    from test_relay_speech_livekit import ControlledVAD

    from speko_gateway.livekit import STT

    observed = []
    source = ControlledVAD()

    def audio(samples):
        return rtc.AudioFrame(
            data=b"\x12\x34" * samples,
            sample_rate=48000,
            num_channels=1,
            samples_per_channel=samples,
        )

    prefix = audio(23 * 48000 if large == "prefix" else 960)
    inference = audio(23 * 48000 if large == "inference" else 960)

    async def handle(request):
        ws = web.WebSocketResponse(max_msg_size=(2 << 20) + 1)
        await ws.prepare(request)
        await ws.receive_json()
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                observed.append(message.data)
            elif json.loads(message.data)["type"] == "session.close":
                await ws.send_json({"type": "session.closed", "usage": {}})
                await ws.close()
        return ws

    async with native_stack(native_binary, handle) as (socket, _):
        client = RelaySpeechClient(
            api_key=MODEL_KEY,
            session_id=str(uuid.uuid4()),
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        model = STT(
            client,
            provider="fixture",
            model="fixture-model",
            credential_source="managed",
            turn_vad=source,
            sample_rate=48000,
        )
        stream = model.stream(manual_turns=True)
        try:
            stream.push_frame(prefix)
            await asyncio.wait_for(source.frames.get(), 3)
            source.processed(prefix)
            source.emit(vad.VADEventType.START_OF_SPEECH)
            assert (
                await asyncio.wait_for(anext(stream), 3)
            ).type is stt.SpeechEventType.START_OF_SPEECH
            stream.push_frame(inference)
            await asyncio.wait_for(source.frames.get(), 3)
            source.processed(inference)
            source.emit(vad.VADEventType.END_OF_SPEECH)
            stream.end_input()
            assert [event.type async for event in stream] == [
                stt.SpeechEventType.END_OF_SPEECH
            ]
            assert len(observed) == 3
            assert all(len(value) <= 2 << 20 for value in observed)
            assert b"".join(observed) == bytes(prefix.data) + bytes(inference.data)
        finally:
            await stream.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("kind", ["stt", "tts"])
async def test_actual_native_sidecar_carries_python_speech_frames_and_session_authority(
    native_binary, kind
):
    seen = []
    pcm = b"\xff\x0f" * 16000

    async def handle(request):
        assert request.path == f"/v1/{kind}/stream"
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        configured = await ws.receive_json()
        seen.append((request.headers.copy(), configured))
        await ready(ws)
        async for message in ws:
            if message.type is aiohttp.WSMsgType.BINARY:
                assert message.data == pcm
                continue
            control = json.loads(message.data)
            if control["type"] == "input.commit":
                if kind == "stt":
                    await ws.send_json(
                        {"type": "transcript.final", "text": "owned synthetic"}
                    )
                else:
                    await ws.send_json({"type": "utterance.started", "sequence": 1})
                    await ws.send_bytes(pcm)
                    await ws.send_json({"type": "utterance.done", "sequence": 1})
            if control["type"] == "session.close":
                await ws.send_json(
                    {"type": "session.closed", "usage": {"duration_ms": 1000}}
                )
                await ws.close()
        return ws

    session_id = str(uuid.uuid4())
    async with native_stack(native_binary, handle) as (socket, _):
        client = RelaySpeechClient(
            api_key=MODEL_KEY,
            session_id=session_id,
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        try:
            if kind == "stt":
                stream = await LiveKitSTTBridge(
                    client,
                    provider="fixture",
                    model="fixture-model",
                    credential_source="managed",
                ).start(SimpleNamespace(sample_rate=16000, num_channels=1))
                await stream.push_frame(SimpleNamespace(data=pcm))
                await stream.flush()
            else:
                stream = await LiveKitTTSBridge(
                    client,
                    provider="fixture",
                    model="fixture-model",
                    credential_source="managed",
                ).start()
                await stream.append_text("owned-private-prompt")
                await stream.commit()
            await stream.finish()
            events = [event async for event in stream.events()]
            if kind == "stt":
                assert [event.text for event in events] == ["owned synthetic"]
            else:
                assert b"".join(event.audio or b"" for event in events) == pcm
        finally:
            await client.aclose()
    headers, configured = seen[0]
    assert headers["Authorization"] == f"Bearer {MODEL_KEY}"
    assert headers["Speko-Client-Session-ID"] == session_id
    assert NATIVE_LOCAL_AUTHORIZATION not in headers
    assert configured["routing"] == {
        "mode": "explicit",
        "provider": "fixture",
        "model": "fixture-model",
    }
    assert len(seen) == 1


async def test_actual_native_sidecar_keeps_two_llm_credentials_isolated_and_streams_sse(
    native_binary,
):
    seen = []
    release = asyncio.Event()

    async def handle(request):
        seen.append((request.headers.copy(), await request.json()))
        response = web.StreamResponse(
            headers={
                "Content-Type": "text/event-stream",
                "Speko-Request-ID": "owned-request",
            }
        )
        await response.prepare(request)
        await response.write(
            b'event: response.output_text.delta\ndata: {"delta":"owned"}\n\n'
        )
        await asyncio.wait_for(release.wait(), 3)
        await response.write(
            b'event: response.completed\ndata: {"status":"completed"}\n\n'
        )
        return response

    sessions = [str(uuid.uuid4()), str(uuid.uuid4())]
    async with native_stack(native_binary, handle) as (socket, _):

        async def run(index):
            client = RelayLLMClient(
                api_key=MODEL_KEY + str(index),
                session_id=sessions[index],
                socket_path=socket,
                local_auth_token=LOCAL_TOKEN,
            )
            try:
                stream = client.stream_response(
                    {
                        "input": [{"content": "owned-private-prompt"}],
                        "max_output_tokens": 64,
                    }
                )
                first = await asyncio.wait_for(anext(stream), 2)
                assert first == ("response.output_text.delta", {"delta": "owned"})
                release.set()
                assert await asyncio.wait_for(anext(stream), 2) == (
                    "response.completed",
                    {"status": "completed"},
                )
                with pytest.raises(StopAsyncIteration):
                    await anext(stream)
            finally:
                await client.aclose()

        await asyncio.gather(run(0), run(1))
    assert sorted(
        (headers["Authorization"], headers["Speko-Client-Session-ID"])
        for headers, _ in seen
    ) == sorted((f"Bearer {MODEL_KEY}{i}", sessions[i]) for i in range(2))
    assert all(NATIVE_LOCAL_AUTHORIZATION not in headers for headers, _ in seen)


async def test_actual_native_sidecar_sigterm_drains_a_live_python_sse_stream(
    native_binary,
):
    release = asyncio.Event()

    async def handle(request):
        await request.read()
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(
            b'event: response.output_text.delta\ndata: {"delta":"owned"}\n\n'
        )
        await asyncio.wait_for(release.wait(), 4)
        await response.write(
            b'event: response.completed\ndata: {"status":"completed"}\n\n'
        )
        return response

    async with native_stack(native_binary, handle) as (socket, process):
        client = RelayLLMClient(
            api_key=MODEL_KEY,
            session_id=str(uuid.uuid4()),
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        try:
            stream = client.stream_response(
                {"input": [{"content": "owned-private-prompt"}]}
            )
            assert await asyncio.wait_for(anext(stream), 2) == (
                "response.output_text.delta",
                {"delta": "owned"},
            )
            process.terminate()
            async with aiohttp.ClientSession(
                connector=aiohttp.UnixConnector(path=socket)
            ) as probe:

                async def wait_until_draining():
                    while True:
                        async with probe.get("http://speko-gateway/readyz") as response:
                            if response.status == 503:
                                return
                        await asyncio.sleep(0.01)

                await asyncio.wait_for(wait_until_draining(), 3)
            assert process.returncode is None
            release.set()
            assert await asyncio.wait_for(anext(stream), 2) == (
                "response.completed",
                {"status": "completed"},
            )
            with pytest.raises(StopAsyncIteration):
                await anext(stream)
            assert await asyncio.wait_for(process.wait(), 4) == 0
        finally:
            release.set()
            await client.aclose()


async def test_actual_native_sidecar_restarts_after_sigkill_with_the_same_socket_volume(
    native_binary,
):
    async def handle(request):
        await request.read()
        return web.Response(
            text='event: response.completed\ndata: {"status":"completed"}\n\n',
            content_type="text/event-stream",
        )

    async with native_stack(native_binary, handle, crash_once=True) as (socket, _):
        client = RelayLLMClient(
            api_key=MODEL_KEY,
            session_id=str(uuid.uuid4()),
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        try:
            events = [event async for event in client.stream_response({"input": []})]
            assert events == [("response.completed", {"status": "completed"})]
        finally:
            await client.aclose()


@pytest.mark.parametrize(
    "status,code", [(402, "insufficient_credit"), (409, "idempotency_conflict")]
)
async def test_native_sidecar_python_speech_retains_canonical_handshake_rejection(
    native_binary,
    status,
    code,
):
    async def handle(request):
        response = web.StreamResponse(
            status=status, headers={"Content-Type": "application/json"}
        )
        await response.prepare(request)
        raw = json.dumps(
            {
                "error": {
                    "code": code,
                    "message": "owned rejection detail",
                    "hint": "owned remediation",
                    "retryable": False,
                    "request_id": "owned-rejection",
                }
            }
        ).encode()
        await response.write(raw[:5])
        await asyncio.sleep(0.01)
        await response.write(raw[5:])
        return response

    async with native_stack(native_binary, handle) as (socket, _):
        from speko_gateway.client import SessionConfig

        client = RelaySpeechClient(
            api_key=MODEL_KEY,
            session_id=str(uuid.uuid4()),
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        try:
            with pytest.raises(GatewayError) as rejection:
                await client.open(
                    SessionConfig(
                        kind="stt",
                        execution={
                            "credential_source": "managed",
                            "provider_route": "speko_relay",
                            "relay_policy": "required",
                        },
                        request={"provider": "fixture", "model": "fixture-model"},
                        media={
                            "encoding": "pcm_s16le",
                            "sample_rate_hz": 16000,
                            "channels": 1,
                        },
                    )
                )
            assert rejection.value.code == code
            assert rejection.value.status == status
            assert rejection.value.request_id == "owned-rejection"
            assert not rejection.value.retryable
            assert "owned rejection detail" not in str(rejection.value)
        finally:
            await client.aclose()


async def test_actual_native_sidecar_rejects_wrong_local_token_before_any_relay_request(
    native_binary,
):
    seen = []

    async def handle(request):
        seen.append(request.path)
        return web.Response(status=500)

    async with native_stack(native_binary, handle) as (socket, _):
        client = RelaySpeechClient(
            api_key=MODEL_KEY,
            session_id=str(uuid.uuid4()),
            socket_path=socket,
            local_auth_token="wrong-local-token",
        )
        try:
            from speko_gateway.client import SessionConfig

            cfg = SessionConfig(
                kind="stt",
                execution={
                    "credential_source": "managed",
                    "provider_route": "speko_relay",
                    "relay_policy": "required",
                },
                request={"provider": "fixture", "model": "fixture-model"},
                media={"encoding": "pcm_s16le", "sample_rate_hz": 16000, "channels": 1},
            )
            with pytest.raises(GatewayError) as error:
                await client.open(cfg)
            assert error.value.code == "http_401"
            assert not error.value.retryable
        finally:
            await client.aclose()
    assert seen == []


async def test_native_sidecar_llm_failure_does_not_replay_the_livekit_request(
    native_binary,
):
    seen = []

    async def handle(request):
        seen.append(request.headers["Idempotency-Key"])
        await request.read()
        return web.Response(
            text='event: error\ndata: {"error":{"code":"provider_failure","retryable":true}}\n\n',
            content_type="text/event-stream",
        )

    async with native_stack(native_binary, handle) as (socket, _):
        client = RelayLLMClient(
            api_key=MODEL_KEY,
            session_id=str(uuid.uuid4()),
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        model = LLM(client, provider="fixture", model="fixture-model")
        context = agents_llm.ChatContext()
        context.add_message(role="user", content="owned-private-prompt")
        options = APIConnectOptions(max_retry=3, retry_interval=0)
        stream = model.chat(chat_ctx=context, conn_options=options)
        try:
            with pytest.raises(APIConnectionError):
                async for _ in stream:
                    pass
            assert seen and len(seen) == 1
            assert options.max_retry == 3
        finally:
            await stream.aclose()
            await model.aclose()
            await client.aclose()


@pytest.mark.parametrize("failure", ["lost-terminal", "retryable-error"])
async def test_native_sidecar_llm_ambiguity_remains_nonretryable(
    native_binary, failure
):
    seen = []

    async def handle(request):
        seen.append(1)
        await request.read()
        text = 'event: response.output_text.delta\ndata: {"delta":"owned"}\n\n'
        if failure == "retryable-error":
            text += 'event: error\ndata: {"error":{"code":"unavailable","retryable":true}}\n\n'
        return web.Response(text=text, content_type="text/event-stream")

    async with native_stack(native_binary, handle) as (socket, _):
        client = RelayLLMClient(
            api_key=MODEL_KEY,
            session_id=str(uuid.uuid4()),
            socket_path=socket,
            local_auth_token=LOCAL_TOKEN,
        )
        try:
            with pytest.raises(RelayError) as rejection:
                async for _ in client.stream_response(
                    {"input": [{"content": "owned-private-prompt"}]}
                ):
                    pass
            assert not rejection.value.retryable
        finally:
            await client.aclose()
    assert seen == [1]
