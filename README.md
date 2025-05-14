# Task Manager Library

Python client library for interacting with the Task Manager API.

## Installation

```bash
pip install git+https://github.com/kakbet/task-manager-lib.git
```

## Usage

```python
from task_manager_lib import TaskManagerClient

# Initialize client
client = TaskManagerClient(base_url="http://localhost:8000")

# Create a task
task = client.create_task(name="example", payload={"key": "value"})

# Get task status
status = client.get_task_status(task.id)
```

## Development

1. Clone the repository
2. Install dependencies:
```bash
pip install -e .
```

3. Run tests:
```bash
pytest
```

## License

MIT
