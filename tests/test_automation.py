from unittest.mock import patch

from sherpamind.automation import doctor_automation, desired_cron_specs


def test_doctor_automation_detects_missing_jobs() -> None:
    with patch('sherpamind.automation.managed_jobs', return_value=[]):
        report = doctor_automation()
    assert len(report['missing']) == len(desired_cron_specs())


def test_doctor_automation_detects_duplicates() -> None:
    with patch('sherpamind.automation.managed_jobs', return_value=[
        {'name': 'sherpamind:hot-open-sync'},
        {'name': 'sherpamind:hot-open-sync'},
    ]):
        report = doctor_automation()
    assert report['duplicates']['sherpamind:hot-open-sync'] == 2
