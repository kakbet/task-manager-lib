import httpx
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel
from functools import wraps

def async_method(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        return await func(self, *args, **kwargs)
    return wrapper

class TaskCreate(BaseModel):
    name: str
    description: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    scheduled_time: Optional[datetime] = None
    priority: Optional[int] = 0
    max_retries: Optional[int] = 3

class Task(BaseModel):
    id: UUID
    name: str
    description: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    status: str = "queued"
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    priority: int = 0
    retries: int = 0
    max_retries: int = 3
    is_locked: bool = False
    locked_by: Optional[str] = None
    scheduled_time: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class TaskUpdate(BaseModel):
    status: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

class TaskManagerClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.client = None

    @async_method
    async def setup(self):
        """Initialize the async client"""
        if self.client is None:
            self.client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={"X-API-Key": self.api_key} if self.api_key else None,
                follow_redirects=True
            )

    @async_method
    async def list_tasks(
        self,
        status: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None
    ) -> List[Task]:
        """List tasks with optional filters"""
        try:
            params = {}
            if status:
                params["status"] = status
            if limit is not None:
                params["limit"] = limit
            if offset is not None:
                params["skip"] = offset

            response = await self.client.get("/tasks/", params=params)
            response.raise_for_status()
            data = response.json()

            if not data:
                return []
            if isinstance(data, dict):
                return [Task.model_validate(data)]
            return [Task.model_validate(item) for item in data]
        except Exception as e:
            print(f"Error in list_tasks: {e}")
            raise

    @async_method
    async def get_task(self, task_id: UUID) -> Task:
        """Get a specific task by ID"""
        try:
            response = await self.client.get(f"/tasks/{task_id}/")
            response.raise_for_status()
            return Task.model_validate(response.json())
        except Exception as e:
            print(f"Error in get_task: {e}")
            raise

    @async_method
    async def create_task(self, task: TaskCreate) -> Task:
        """Create a new task"""
        try:
            response = await self.client.post("/tasks/", json=task.model_dump())
            response.raise_for_status()
            return Task.model_validate(response.json())
        except Exception as e:
            print(f"Error in create_task: {e}")
            raise

    @async_method
    async def update_task(self, task_id: UUID, update: TaskUpdate) -> bool:
        """Update a task"""
        try:
            response = await self.client.put(
                f"/tasks/{task_id}",
                json=update.model_dump(exclude_unset=True)
            )
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Error in update_task: {e}")
            raise

    @async_method
    async def lock_task(self, task_id: UUID, worker_id: str) -> bool:
        """Lock a task for processing"""
        try:
            response = await self.client.post(
                f"/tasks/{task_id}/lock",
                json={"worker_id": worker_id}
            )
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Error in lock_task: {e}")
            raise

    @async_method
    async def unlock_task(self, task_id: UUID) -> bool:
        """Unlock a task"""
        try:
            response = await self.client.post(f"/tasks/{task_id}/unlock")
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Error in unlock_task: {e}")
            raise

    async def close(self):
        """Close the async client"""
        if self.client:
            await self.client.aclose()
            self.client = None
