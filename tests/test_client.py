import httpx

from sherpamind.client import is_retryable_http_error


def _status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request('GET', f'https://api.sherpadesk.com/test/{status_code}')
    response = httpx.Response(status_code, request=request)
    return httpx.HTTPStatusError(f'status {status_code}', request=request, response=response)


def test_is_retryable_http_error_retries_transient_http_statuses() -> None:
    assert is_retryable_http_error(_status_error(429)) is True
    assert is_retryable_http_error(_status_error(503)) is True


def test_is_retryable_http_error_skips_non_retryable_http_statuses() -> None:
    assert is_retryable_http_error(_status_error(400)) is False
    assert is_retryable_http_error(_status_error(401)) is False
    assert is_retryable_http_error(_status_error(403)) is False
    assert is_retryable_http_error(_status_error(404)) is False


def test_is_retryable_http_error_retries_transport_errors() -> None:
    request = httpx.Request('GET', 'https://api.sherpadesk.com/test')
    exc = httpx.ConnectError('boom', request=request)
    assert is_retryable_http_error(exc) is True
