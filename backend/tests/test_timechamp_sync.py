import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock
from app.integrations.timechamp.models import TrackingSummaryRecord
from app.integrations.timechamp.transformer import TrackingDataTransformer
from app.integrations.timechamp.pipeline import TrackingSyncPipeline


def test_tracking_data_transformer():
    transformer = TrackingDataTransformer()
    raw_sample = {
        "empID": "W9999",
        "username": "test.user@wyzmindz.com",
        "name": "Test User",
        "departmentName": "Engineering",
        "reportingHeadName": "Supervisor A",
        "timesheetDate": "14-Oct-26",
        "startDateTime": "14/10/2026 09:00",
        "finishDateTime": "14/10/2026 18:00",
        "totalTime": 32400,
        "workingTime": 28800,
        "productiveTime": 25200,
        "idleTime": 3600,
        "unproductiveTime": 0,
        "neutralTime": 0,
    }

    record: TrackingSummaryRecord = transformer.transform_record(raw_sample)
    assert record.emp_id == "W9999"
    assert record.name == "Test User"
    assert record.department_name == "Engineering"
    assert record.timesheet_date == date(2026, 10, 14)
    assert record.total_time == "09:00:00"
    assert record.total_time_seconds == 32400
    assert record.productive_hours == "07:00:00"
    assert record.productive_time_seconds == 25200
    assert record.working_hours == "08:00:00"
    assert record.working_time_seconds == 28800


@pytest.mark.asyncio
async def test_tracking_sync_pipeline_success():
    mock_client = MagicMock()
    mock_client.get_tracking_summary_data = AsyncMock(return_value=[
        {
            "empID": "W1234",
            "username": "emp@company.com",
            "timesheetDate": "14-Oct-26",
            "totalTime": 1000,
            "workingTime": 900,
            "productiveTime": 800,
            "departmentName": "QA",
        }
    ])

    mock_repo = MagicMock()
    mock_repo.save_batch = AsyncMock(return_value=1)
    mock_repo.save_sync_history = AsyncMock(return_value=None)

    transformer = TrackingDataTransformer()
    pipeline = TrackingSyncPipeline(
        client=mock_client,
        transformer=transformer,
        repository=mock_repo,
    )

    result = await pipeline.run_sync(sync_type="UNIT_TEST", target_date="14-Oct-26")
    assert result.success is True
    assert result.records_fetched == 1
    assert result.records_inserted == 1
    mock_repo.save_batch.assert_called_once()
    mock_repo.save_sync_history.assert_called_once()


def test_payroll_cycle_range_calculation():
    from app.integrations.timechamp.pipeline import get_payroll_cycle_range
    # Test specific cycle month
    start, end = get_payroll_cycle_range(cycle_str="2026-10")
    assert start == date(2026, 9, 26)
    assert end == date(2026, 10, 25)

    # Test boundary on 25th (belongs to current ending cycle)
    start_25, end_25 = get_payroll_cycle_range(ref_date=date(2026, 10, 25))
    assert start_25 == date(2026, 9, 26)
    assert end_25 == date(2026, 10, 25)

    # Test boundary on 26th (belongs to next starting cycle)
    start_26, end_26 = get_payroll_cycle_range(ref_date=date(2026, 10, 26))
    assert start_26 == date(2026, 10, 26)
    assert end_26 == date(2026, 11, 25)

    # Test January rollover
    start_jan, end_jan = get_payroll_cycle_range(cycle_str="2027-01")
    assert start_jan == date(2026, 12, 26)
    assert end_jan == date(2027, 1, 25)


def test_catchup_check_window_calculation():
    from app.integrations.timechamp.pipeline import get_catchup_check_window

    # Day >= 26 (e.g. Oct 26: should check back to Sep 26)
    start_oct26, end_oct26 = get_catchup_check_window(ref_date=date(2026, 10, 26))
    assert start_oct26 == date(2026, 9, 26)
    assert end_oct26 == date(2026, 10, 26)

    # Day < 26 (e.g. Oct 15: should check back to Aug 26)
    start_oct15, end_oct15 = get_catchup_check_window(ref_date=date(2026, 10, 15))
    assert start_oct15 == date(2026, 8, 26)
    assert end_oct15 == date(2026, 10, 15)

    # January rollover (Jan 5: should check back to Nov 26 of prior year)
    start_jan5, end_jan5 = get_catchup_check_window(ref_date=date(2027, 1, 5))
    assert start_jan5 == date(2026, 11, 26)
    assert end_jan5 == date(2027, 1, 5)


@pytest.mark.asyncio
async def test_pipeline_auto_catchup_detects_and_backfills_missing_dates():
    from app.integrations.timechamp.pipeline import TrackingSyncPipeline

    mock_client = MagicMock()
    mock_client.get_tracking_summary_data = AsyncMock(return_value=[
        {"empID": "W001", "username": "u@test.com", "timesheetDate": "20-Oct-26", "totalTime": 100}
    ])

    mock_repo = MagicMock()
    mock_repo.ensure_historical_data_restored = AsyncMock(return_value=16308)
    mock_repo.get_missing_dates = AsyncMock(return_value=[date(2026, 10, 20)])
    mock_repo.save_batch = AsyncMock(return_value=1)
    mock_repo.save_sync_history = AsyncMock(return_value=None)

    transformer = TrackingDataTransformer()
    pipeline = TrackingSyncPipeline(
        client=mock_client,
        transformer=transformer,
        repository=mock_repo,
    )

    result = await pipeline.run_sync(sync_type="STARTUP_AUTO_CATCHUP", auto_catchup=True)
    assert result.success is True
    # Verify ensure_historical_data_restored was executed
    mock_repo.ensure_historical_data_restored.assert_called_once()
    # Verify get_missing_dates was executed
    mock_repo.get_missing_dates.assert_called_once()
    # Verify missing date (20-Oct-26) was fetched
    called_dates = [call.kwargs.get("target_date") for call in mock_client.get_tracking_summary_data.call_args_list]
    assert "20-Oct-26" in called_dates
