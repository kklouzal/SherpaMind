import base64

from sherpamind.client import SherpaDeskClient


def test_build_headers_uses_basic_auth_for_org_instance() -> None:
    client = SherpaDeskClient(
        api_base_url="https://api.sherpadesk.com",
        api_key="secret",
        org_key="org1",
        instance_key="main1",
        min_interval_seconds=0,
    )
    headers = client._build_headers()
    expected = base64.b64encode(b"org1-main1:secret").decode("ascii")
    assert headers["Authorization"] == f"Basic {expected}"
    assert headers["Accept"] == "application/json"


def test_build_headers_can_use_discovery_identity() -> None:
    client = SherpaDeskClient(
        api_base_url="https://api.sherpadesk.com",
        api_key="secret",
        min_interval_seconds=0,
    )
    headers = client._build_headers()
    expected = base64.b64encode(b"x:secret").decode("ascii")
    assert headers["Authorization"] == f"Basic {expected}"


def test_build_url_normalizes_slashes() -> None:
    client = SherpaDeskClient(
        api_base_url="https://api.sherpadesk.com/",
        api_key="secret",
        min_interval_seconds=0,
    )
    assert client._build_url("/tickets") == "https://api.sherpadesk.com/tickets"


def test_list_paginated_aggregates_pages() -> None:
    client = SherpaDeskClient(api_base_url="https://api.sherpadesk.com", api_key="secret", min_interval_seconds=0)
    seen = []

    def fake_get(path, params=None):
        seen.append((path, params))
        if params["page"] == 0:
            return [{"id": 1}, {"id": 2}]
        return [{"id": 3}]

    client.get = fake_get  # type: ignore[method-assign]
    rows = client.list_paginated("tickets", page_size=2)
    assert [row["id"] for row in rows] == [1, 2, 3]
    assert seen[0][1] == {"limit": 2, "page": 0}
    assert seen[1][1] == {"limit": 2, "page": 1}
