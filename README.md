# Task Manager Library

This library contains shared components used by the Task Manager application, including:

- Data models for tasks, logs, and locks
- Common utilities and helpers
- Shared configurations

## Installation

```bash
pip install -e .
```

## Usage

```python
from task_manager_lib.models import Task, TaskCreate, TaskUpdate

# Create a new task
task = TaskCreate(
    name="example_task",
    description="An example task",
    priority=50,
    max_retries=3
)

# Update task
update = TaskUpdate(
    status="running",
    priority=75
)
```

## Models

### Task
- `name`: Task name (required)
- `description`: Task description
- `parameters`: Task parameters (JSON)
- `scheduled_time`: When to run the task
- `priority`: Task priority (0-100, higher is more important)
- `max_retries`: Maximum retry attempts
- `retries`: Current retry count
- `status`: Current task status
- `result`: Task result (JSON)
- `error`: Error message if failed

### TaskLock
- `task_id`: ID of locked task
- `locked_by`: Worker ID that locked the task
- `locked_at`: When the task was locked
- `is_locked`: Current lock status

### TaskLog
- `task_id`: Task ID
- `log_level`: Log level (INFO, ERROR, etc.)
- `message`: Log message
- `context`: Additional context (JSON)
