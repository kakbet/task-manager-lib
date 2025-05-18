"""
Task Manager API Client
"""
import logging
from typing import Dict, List, Optional, Any, Union
import httpx
from .models import Task, TaskCreate, TaskUpdate

logger = logging.getLogger(__name__)

class TaskManagerClient:
    """Client for interacting with Task Manager API"""
    
    def __init__(self, base_url: str, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            headers={
                "Content-Type": "application/json",
                "X-API-Key": api_key or ""
            }
        )
        self.worker_id = "default_worker"
    
    async def _handle_response(self, response: httpx.Response) -> Any:
        """Handle API response and errors"""
        try:
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Error processing response: {e}")
            raise

    async def list_tasks(
        self,
        status: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Task]:
        """List tasks with optional filters"""
        try:
            params = {}
            if status:
                params["status"] = status
            if limit:
                params["limit"] = limit

            response = await self.client.get("/tasks/", params=params)
            data = await self._handle_response(response)
            
            if not isinstance(data, list):
                logger.error(f"Unexpected response format: {data}")
                return []
                
            return [Task.model_validate(task) for task in data]
        except Exception as e:
            logger.error(f"Error in list_tasks: {e}")
            return []

    async def get_task(self, task_id: str) -> Optional[Task]:
        """Get a specific task by ID"""
        try:
            response = await self.client.get(f"/tasks/{task_id}")
            data = await self._handle_response(response)
            return Task.model_validate(data)
        except Exception as e:
            logger.error(f"Error in get_task: {e}")
            return None

    async def create_task(self, task: TaskCreate) -> Optional[Task]:
        """Create a new task"""
        try:
            response = await self.client.post("/tasks", json=task.model_dump())
            data = await self._handle_response(response)
            return Task.model_validate(data)
        except Exception as e:
            logger.error(f"Error in create_task: {e}")
            return None

    async def update_task(
        self,
        task_id: str,
        status: Optional[str] = None,
        result: Optional[Dict] = None,
        error: Optional[str] = None
    ) -> Optional[Task]:
        """Update task status and result"""
        try:
            update_data = TaskUpdate(
                status=status,
                result=result,
                error=error
            )
            response = await self.client.put(
                f"/tasks/{task_id}",
                json=update_data.model_dump(exclude_none=True)
            )
            data = await self._handle_response(response)
            return Task.model_validate(data)
        except Exception as e:
            logger.error(f"Error in update_task: {e}")
            return None

    def set_worker_id(self, worker_id: str):
        """Set worker ID for this client"""
        self.worker_id = worker_id

    async def lock_task(self, task_id: str, worker_id: Optional[str] = None) -> bool:
        """Lock a task for processing"""
        try:
            response = await self.client.post(
                f"/tasks/{task_id}/lock",
                json={"worker_id": worker_id or self.worker_id}
            )
            data = await self._handle_response(response)
            return "Task locked successfully" in data.get("message", "")
        except Exception as e:
            logger.error(f"Error in lock_task: {e}")
            return False

    async def unlock_task(self, task_id: str, worker_id: Optional[str] = None) -> bool:
        """Unlock a task"""
        try:
            response = await self.client.post(
                f"/tasks/{task_id}/unlock",
                json={"worker_id": worker_id or self.worker_id}
            )
            data = await self._handle_response(response)
            return "Task unlocked successfully" in data.get("message", "")
        except Exception as e:
            logger.error(f"Error in unlock_task: {e}")
            return False

    async def send_heartbeat(self, task_id: str, worker_id: Optional[str] = None) -> bool:
        """Send heartbeat for a locked task"""
        try:
            response = await self.client.post(
                f"/tasks/{task_id}/heartbeat",
                json={"worker_id": worker_id or self.worker_id}
            )
            data = await self._handle_response(response)
            return "Heartbeat updated successfully" in data.get("message", "")
        except Exception as e:
            logger.error(f"Error in send_heartbeat: {e}")
            return False

    async def close(self):
        """Close the client session"""
        await self.client.aclose()
