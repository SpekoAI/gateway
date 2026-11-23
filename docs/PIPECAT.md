# Use Speko Gateway with Pipecat

This guide adds Speko Gateway as a sidecar inside a Pipecat cascade agent
(speech-to-text, then an LLM, then text-to-speech). The Python package
`speko-gateway` implements Pipecat's `STTService`, `LLMService` and
`TTSService`. It needs `pipecat-ai>=1.7,<2` and Python 3.11 or newer.

On provider-direct routes, voice audio goes from the Gateway straight to the
speech providers. `SpekoLLMService` sends the conversation text to the hosted
Speko Router. By default it reads `SPEKO_API_KEY`.

The steps follow the project that the
[`pipecat init` CLI](https://docs.pipecat.ai/api-reference/cli/init) creates,
with its `dailyco/pipecat-base` Dockerfile.

## 1. Replace the model services

In `bot.py`, replace the provider imports:

```diff
-from pipecat.services.cartesia.tts import CartesiaTTSService
-from pipecat.services.deepgram.stt import DeepgramSTTService
-from pipecat.services.openai.llm import OpenAILLMService
+from speko_gateway.pipecat import SpekoLLMService, SpekoSTTService, SpekoTTSService
```

Then build the three services inside `run_bot`:

```python
stt = SpekoSTTService(
    language="en",
    provider="auto",
    model="auto",
    credential_source="auto",  # or "byok" / "managed"
)
llm = SpekoLLMService(
    provider="auto",
    model="auto",
    session_id=platform_session_id,  # optional, see below
)
tts = SpekoTTSService(
    language="en",
    provider="auto",
    model="auto",
    voice="",                  # the configured or catalog default
    credential_source="auto",  # or "byok" / "managed"
)
```

The pipeline stays the standard Pipecat cascade:

```python
pipeline = Pipeline(
    [
        transport.input(),
        stt,
        user_aggregator,
        llm,
        tts,
        transport.output(),
        assistant_aggregator,
    ]
)
```

Keep the generated `SileroVADAnalyzer` on the user aggregator, or another
Pipecat VAD that emits `VADUserStoppedSpeakingFrame`. The STT service uses
that frame to commit each utterance.

You can keep any other Pipecat LLM service instead of `SpekoLLMService`.

## 2. Add the sidecar to the image

The Gateway image holds both the binary and the Python package. Add these
lines to the Dockerfile from `pipecat init`:

```diff
+FROM spekoai/gateway:0.1 AS speko-gateway
 FROM dailyco/pipecat-base:latest

 ENV UV_COMPILE_BYTECODE=1
 ENV UV_LINK_MODE=copy

 RUN --mount=type=cache,target=/root/.cache/uv \
     --mount=type=bind,source=uv.lock,target=uv.lock \
     --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
     uv sync --locked --no-install-project --no-dev

+COPY --from=speko-gateway /opt/speko/python /opt/speko/python
+RUN uv pip install --python /app/.venv/bin/python /opt/speko/python
+COPY --from=speko-gateway /usr/local/bin/speko-gateway /usr/local/bin/speko-gateway
+RUN install -d -m 0700 /run/speko
+COPY ./pre-app.sh /app/pre-app.sh

 COPY ./bot.py bot.py
```

Add `pre-app.sh` beside the Dockerfile:

```sh
#!/bin/sh
set -eu

/usr/local/bin/speko-gateway &
```

Do not add or replace the image `CMD`. Pipecat's base image owns the command
that serves `bot.py`, and its
[Dockerfile](https://github.com/daily-co/pipecat-cloud-images/blob/main/pipecat-base/Dockerfile)
runs `/app/pre-app.sh` first.

The Speko services wait for the Gateway to become ready for up to 15 seconds
(`ready_timeout=`), then fail the pipeline with a clear error.

For production, pin both base images to a version or digest.

## 3. Configure credentials

Generate one token that only the two processes in the container share:

```bash
openssl rand -hex 32
```

Then choose managed routing, BYOK, or both in `.env`.

Managed routing and billing:

```dotenv
SPEKO_LOCAL_AUTH_TOKEN=replace-with-generated-token
SPEKO_API_KEY=replace-with-speko-api-key
```

BYOK, for example:

```dotenv
SPEKO_LOCAL_AUTH_TOKEN=replace-with-generated-token
SPEKO_DEEPGRAM_BYOK_API_KEY=replace-with-deepgram-key
SPEKO_CARTESIA_BYOK_API_KEY=replace-with-cartesia-key
```

- `credential_source="auto"` uses managed routing when `SPEKO_API_KEY` is set
  and BYOK otherwise.
- With more than one BYOK key, pin the provider on each voice service
  (`provider="deepgram"` on STT, `provider="cartesia"` on TTS). Deepgram and
  Cartesia both serve STT and TTS, so `provider="auto"` is refused.
- `SpekoLLMService()` needs `SPEKO_API_KEY`, unless you pass it your own
  `client=RelayLLMClient(...)` with another credential. A BYOK-only `.env`
  runs the voice services, not the default LLM.
- Set `credential_source="byok"` when both are set but voice must stay on
  your own keys.
- With BYOK, only the Gateway process reads the provider keys. The Pipecat
  services use only the local token. Both processes share one container, so
  use separate containers and a shared socket volume when the keys must be
  hidden from the bot process.

All variables and the `_FILE` secret forms are in the
[configuration table](../README.md#configuration).

## 4. Run

```bash
docker build -t my-pipecat-speko-agent .
docker run --rm --env-file .env -p 7860:7860 my-pipecat-speko-agent
```

Open `http://localhost:7860`, connect, and check the first turn. The services
emit the standard Pipecat transcription, TTS audio, usage, TTFB and TTFA
frames, so existing Pipecat observers keep working.

## How it behaves

- STT: one long-lived Gateway stream per pipeline.
- TTS: one Gateway session per bot turn. The first sentence is sent at once,
  and each next sentence waits for the Gateway's `audio.done`. A Pipecat
  interruption cancels the turn on the Gateway and releases a waiting
  sentence.
- LLM: streaming text, function calls and results, context and tool updates,
  cancellation, usage metrics and out-of-band `run_inference`.
- Pipecat supplies the pipeline sample rates at startup, so `sample_rate` is
  normally unset. A custom transport can pass `sample_rate=` and
  `num_channels=`. Gateway audio is signed 16-bit little-endian PCM.
- Pass the same `session_id` to all three services to tie their requests to
  one conversation in Speko.
- All services close their streams and HTTP clients when Pipecat shuts down.

## Options

STT options:

```python
stt = SpekoSTTService(
    provider="deepgram",
    model="nova-3",
    diarization=True,
    keywords=["Speko", "Pipecat"],
    provider_options={
        "deepgram": {"numerals": True, "endpointing": 1200},
    },
)
```

TTS takes the vendor's own synthesis settings the same way:

```python
tts = SpekoTTSService(
    provider="cartesia",
    model="sonic-3.6",
    language="hi",
    provider_options={"cartesia": {"normalization": "en-IN"}},
)
```

| Provider | TTS setting | Models and range |
| --- | --- | --- |
| Cartesia | `normalization` | `sonic-3.6` |
| Cartesia | `max_buffer_delay_ms` | `sonic-3`, `sonic-3.5`, `sonic-3.6`. 0 to 5000, streaming only |
| Cartesia | `speed` | `sonic-3`, `sonic-3.5`, `sonic-3.6`. 0.6 to 1.5 |
| ElevenLabs | `stability`, `similarity_boost`, `style`, `speed`, `use_speaker_boost` | |

Options fail closed: a provider that cannot honor one refuses the session.
Pin both `provider` and `model` when an option is a requirement. With
`auto`, routing may pick a provider that refuses it.

### LLM reasoning and penalties

```python
from pipecat.frames.frames import LLMUpdateSettingsFrame

llm = SpekoLLMService(provider="openai", model="gpt-5.6-luna", reasoning_effort="none")

await task.queue_frame(
    LLMUpdateSettingsFrame(delta=SpekoLLMService.Settings(reasoning_effort="low"))
)
```

`reasoning_effort` and the penalties `frequency_penalty`, `presence_penalty`
and `repetition_penalty` work as in the
[Python package README](../integrations/python/README.md#speko-llm). All four
can change at runtime with `LLMUpdateSettingsFrame`. `frequency_penalty` and
`presence_penalty` are Pipecat's own `LLMSettings` fields.
`repetition_penalty` and `reasoning_effort` are fields of
`SpekoLLMService.Settings`.

### Fallback to automatic routing

`fallback_to_auto_on_no_eligible_route=True` on `SpekoSTTService` and
`SpekoTTSService` retries once with automatic provider and model selection
when a pinned provider or model is refused with `no_eligible_route`.

- It applies to managed routing only (`credential_source="managed"`, or
  `"auto"` that resolves to managed).
- TTS drops the configured voice on fallback, because voices belong to one
  provider. Later turns on the same TTS service stay on automatic routing.
- It is off by default and changes no other error handling.
