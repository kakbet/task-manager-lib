from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID

import requests
from pydantic import BaseModel

class TaskCreate(BaseModel):
    name: str
    description: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    scheduled_time: Optional[datetime] = None

class TaskLog(BaseModel):
    id: UUID
    task_id: UUID
    log_time: datetime
    log_level: str
    message: str
    context: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True

class TaskLock(BaseModel):
    task_id: UUID
    locked_by: str
    locked_at: datetime

    class Config:
        from_attributes = True

class Task(TaskCreate):
    id: UUID
    status: str
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    logs: List[TaskLog] = []
    lock: Optional[TaskLock] = None

    @classmethod
    def model_validate(cls, obj):
        if isinstance(obj, dict):
            # Convert string dates to datetime objects
            for field in ['created_at', 'updated_at', 'started_at', 'completed_at']:
                if obj.get(field):
                    obj[field] = datetime.fromisoformat(obj[field].replace('Z', '+00:00'))
            return super().model_validate(obj)
        return super().model_validate(obj)

    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None,
            UUID: lambda v: str(v)
        }

class TaskManagerClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({"Authorization": f"Bearer {api_key}"})

    def create_task(self, task: TaskCreate) -> Task:
        """Create a new task"""
        response = self.session.post(
            f"{self.base_url}/tasks/",
            json=task.model_dump(exclude_unset=True)
        )
        response.raise_for_status()
        return Task.model_validate(response.json())

    def get_task(self, task_id: UUID) -> Task:
        """Get task by ID"""
        response = self.session.get(f"{self.base_url}/tasks/{task_id}")
        response.raise_for_status()
        return Task.model_validate(response.json())

    def list_tasks(
        self,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 100
    ) -> List[Task]:
        """List tasks with optional filtering"""
        params = {"skip": skip, "limit": limit}
        if status:
            params["status"] = status
        
        response = self.session.get(f"{self.base_url}/tasks/", params=params)
        response.raise_for_status()
        return [Task.model_validate(t) for t in response.json()]

    def update_task(self, task_id: UUID, task_data: Dict[str, Any]) -> Task:
        """Update a task"""
        response = self.session.put(f"{self.base_url}/tasks/{task_id}", json=task_data)
        response.raise_for_status()
        return Task.model_validate(response.json())

    def cancel_task(self, task_id: UUID) -> Dict[str, str]:
        """Cancel a task"""
        response = self.session.post(f"{self.base_url}/tasks/{task_id}/cancel")
        response.raise_for_status()
        return response.json()

    def pause_task(self, task_id: UUID) -> Dict[str, str]:
        """Pause a running task"""
        response = self.session.post(f"{self.base_url}/tasks/{task_id}/pause")
        response.raise_for_status()
        return response.json()

    def resume_task(self, task_id: UUID) -> Dict[str, str]:
        """Resume a paused task"""
        response = self.session.post(f"{self.base_url}/tasks/{task_id}/resume")
        response.raise_for_status()
        return response.json()

    def retry_task(self, task_id: UUID) -> Dict[str, str]:
        """Retry a failed task"""
        response = self.session.post(f"{self.base_url}/tasks/{task_id}/retry")
        response.raise_for_status()
        return response.json()

    def get_task_logs(
        self,
        task_id: UUID,
        skip: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get logs for a specific task"""
        response = self.session.get(
            f"{self.base_url}/tasks/{task_id}/logs",
            params={"skip": skip, "limit": limit}
        )
        response.raise_for_status()
        return response.json()
