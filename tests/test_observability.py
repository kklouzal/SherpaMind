from pathlib import Path

from sherpamind.db import initialize_db, record_api_request_event
from sherpamind.observability import generate_runtime_status_artifacts


def test_generate_runtime_status_artifacts(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv('SHERPAMIND_WORKSPACE_ROOT', str(tmp_path))
    db = tmp_path / '.SherpaMind' / 'private' / 'data' / 'sherpamind.sqlite3'
    initialize_db(db)
    record_api_request_event(
        db,
        method='GET',
        path='tickets',
        status_code=404,
        outcome='http_response',
        extra={
            'detail': "Client error '404 User with this token was not found.' for url 'https://api.sherpadesk.com/tickets'",
            'response_body_preview': 'User with this token was not found.',
        },
    )
    result = generate_runtime_status_artifacts(db)
    assert result['status'] == 'ok'
    out = Path(result['output_path'])
    assert out.exists()
    text = out.read_text()
    assert 'SherpaMind Runtime Status' in text
    assert 'Sync freshness summary' in text
    assert 'Sync freshness lanes' in text
    assert 'Vector index status' in text
    assert 'API failure diagnosis' in text
    assert 'API failure signatures' in text
    assert 'Invalid or unknown API token' in text
    assert 'Retrieval readiness' in text
