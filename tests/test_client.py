from sherpamind.client import SherpaDeskClient


def test_build_headers_includes_api_key_and_optional_user() -> None:
    client = SherpaDeskClient(
        base_url="https://example.sherpadesk.test",
        api_key="secret",
        api_user="operator@example.com",
        min_interval_seconds=0,
    )
    headers = client._build_headers()
    assert headers["X-API-Key"] == "secret"
    assert headers["X-API-User"] == "operator@example.com"
    assert headers["Accept"] == "application/json"


def test_build_url_normalizes_slashes() -> None:
    client = SherpaDeskClient(
        base_url="https://example.sherpadesk.test/api/",
        api_key="secret",
        min_interval_seconds=0,
    )
    assert client._build_url("/tickets") == "https://example.sherpadesk.test/api/tickets"
