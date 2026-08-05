# Contributing

Thank you for helping improve Speko Gateway.

1. Open an issue for substantial behavior or protocol changes so the trust and
   compatibility implications can be discussed first.
2. Keep provider-specific behavior inside its adapter and preserve the
   provider-neutral public protocol.
3. Never add live credentials, customer content, production account IDs, or
   private Speko infrastructure details to code, fixtures, logs, or examples.
4. Add tests for behavior and for negative security cases.
5. Run `make check` before opening a pull request.

Changes that affect outbound data, authentication, plan verification, endpoint
selection, or telemetry must update `docs/TRUST.md` in the same pull request.

By contributing, you agree that your contribution is licensed under the MIT
License in this repository.
