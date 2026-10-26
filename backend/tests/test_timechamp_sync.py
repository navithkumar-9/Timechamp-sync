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
