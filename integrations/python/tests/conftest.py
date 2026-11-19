def pytest_addoption(parser):
    parser.addoption(
        "--native-gateway-image",
        default=None,
        help="Run native sidecar transport tests against this local Docker image",
    )
