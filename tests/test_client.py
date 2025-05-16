"""
Tests for TaskManagerClient
"""
import pytest
from task_manager_lib.client import TaskManagerClient

@pytest.mark.asyncio
async def test_list_tasks():
    """Test listing tasks"""
    client = TaskManagerClient(base_url="http://localhost:8000/api/v1")
    tasks = await client.list_tasks(status="pending", limit=1)
    assert isinstance(tasks, list)

@pytest.mark.asyncio
async def test_update_task():
    """Test updating task status"""
    client = TaskManagerClient(base_url="http://localhost:8000/api/v1")
    result = await client.update_task(
        task_id="test-id",
        status="completed",
        result={"value": 42}
    )
    assert result is not None
