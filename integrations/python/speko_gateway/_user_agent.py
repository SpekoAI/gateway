"""Package identity for the owned Router and local Gateway transports."""

from importlib.metadata import PackageNotFoundError, version

try:
    USER_AGENT = f"speko-gateway/{version('speko-gateway')}"
except PackageNotFoundError:
    USER_AGENT = "speko-gateway/unknown"
