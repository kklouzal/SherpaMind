from sherpamind.text_cleanup import normalize_ticket_text, summarize_resolution_from_logs


def test_normalize_ticket_text_strips_html_parser_noise_and_urls() -> None:
    raw = "<p>Hello</p><br>This ticket was created via the email parser.<br>Following file was uploaded: abc.png<br><table><tr><td>sig</td></tr></table>https://example.com"
    cleaned = normalize_ticket_text(raw)
    assert "Hello" in cleaned
    assert "email parser" not in cleaned.lower()
    assert "uploaded" not in cleaned.lower()
    assert "https://" not in cleaned


def test_summarize_resolution_from_logs_returns_first_segment() -> None:
    text = "Closed successfully --- older note"
    assert summarize_resolution_from_logs(text).startswith("Closed successfully")
