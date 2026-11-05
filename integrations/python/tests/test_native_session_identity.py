import pytest

from speko_gateway import RelayLLMClient, RelaySpeechClient


@pytest.mark.parametrize("client", [RelaySpeechClient, RelayLLMClient])
@pytest.mark.parametrize(
    "session_id",
    ["", "a" * 201, "_session", "session/name", "session,name", "two sessions", "é", "a\r\nb"],
)
async def test_native_client_rejects_invalid_platform_identity(client, session_id):
    with pytest.raises(ValueError, match="platform session identifier"):
        client(
            api_key="sk_speko_agent_fixture",
            session_id=session_id,
            socket_path="/tmp/owned.sock",
            local_auth_token="owned-local-token",
        )
