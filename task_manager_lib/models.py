from datetime import datetime
from typing import Optional, Dict, Any
from uuid import UUID
from pydantic import BaseModel, Field

class TaskBase(BaseModel):
    name: str
    description: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    scheduled_time: Optional[datetime] = None
    priority: Optional[int] = Field(default=0, ge=0, description="Task priority (0-100, higher is more important)")
    max_retries: Optional[int] = Field(default=3, ge=0, description="Maximum number of retry attempts")

class TaskCreate(TaskBase):
    pass

class TaskUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    scheduled_time: Optional[datetime] = None
    status: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    priority: Optional[int] = Field(None, ge=0, description="Task priority (0-100, higher is more important)")
    max_retries: Optional[int] = Field(None, ge=0, description="Maximum number of retry attempts")

class Task(TaskBase):
    id: UUID
    status: str
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retries: int = 0
    max_retries: int = 3

class TaskLogBase(BaseModel):
    task_id: UUID
    log_level: str
    message: str
    context: Optional[Dict[str, Any]] = None

class TaskLogCreate(TaskLogBase):
    pass

class TaskLog(TaskLogBase):
    id: UUID
    log_time: datetime

class TaskLockBase(BaseModel):
    task_id: UUID
    locked_by: str

class TaskLock(TaskLockBase):
    locked_at: datetime
    is_locked: bool = True

# Response Models
class TaskResponse(Task):
    logs: list[TaskLog] = []
    lock: Optional[TaskLock] = None

class TaskStatusCount(BaseModel):
    status: str
    count: int

class TaskMonitoringResponse(BaseModel):
    total_tasks: int
    status_counts: list[TaskStatusCount]
    running_tasks: list[Task]
