"""TaskManager API client implementation"""
import logging
import json
from typing import List, Optional, Dict, Any
import httpx
from datetime import datetime

from .models import Task, TaskCreate, TaskUpdate

logger = logging.getLogger(__name__)

class TaskManagerAPIError(Exception):
    """Base exception for API errors"""
    def __init__(self, message: str, status_code: int = None, response_body: str = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body

class TaskManagerClient:
    def __init__(self, base_url: str, timeout: int = 30):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.worker_id = None
        self.client = httpx.AsyncClient(
            timeout=timeout,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=30),
            headers={"Content-Type": "application/json"}
        )
        self._last_error_time = None
        self._error_count = 0

    async def _make_request(self, method: str, path: str, **kwargs) -> Any:
        """Make HTTP request with error handling and logging"""
        url = f"{self.base_url}{path}"
        current_time = datetime.utcnow()
        
        try:
            response = await self.client.request(method, url, **kwargs)
            
            # Log request details at DEBUG level
            logger.debug(f"API Request: {method} {url}")
            logger.debug(f"Request body: {kwargs.get('json')}")
            
            # Reset error counter on successful request
            if response.status_code < 400:
                self._error_count = 0
                self._last_error_time = None
                
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                error_body = response.text
                try:
                    error_json = response.json()
                    if isinstance(error_json, dict):
                        error_body = error_json.get('detail', error_body)
                except:
                    pass

                # Log detailed error information
                logger.error(f"API Error: {method} {url} returned {response.status_code}")
                logger.error(f"Response body: {error_body}")
                
                # Track error frequency
                self._error_count += 1
                self._last_error_time = current_time
                
                if response.status_code == 500:
                    logger.error("Internal Server Error detected. This might indicate a database or application error.")
                
                raise TaskManagerAPIError(
                    f"API request failed: {error_body}",
                    status_code=response.status_code,
                    response_body=error_body
                )

            return response.json() if response.content else None

        except httpx.RequestError as e:
            # Connection/network level errors
            self._error_count += 1
            self._last_error_time = current_time
            
            logger.error(f"Network error during {method} {url}: {str(e)}")
            raise TaskManagerAPIError(f"Network error: {str(e)}")

    def set_worker_id(self, worker_id: str) -> None:
        """Set worker ID for the client"""
        self.worker_id = worker_id
        logger.info(f"Worker ID set to: {worker_id}")

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

            response = await self._make_request("GET", "/tasks/", params=params)
            data = response
            
            if not isinstance(data, list):
                logger.error(f"Unexpected response format: {data}")
                return []
                
            return [Task.model_validate(task) for task in data]
        except Exception as e:
            logger.error(f"Error in list_tasks: {str(e)}\n{traceback.format_exc()}")
            return []

    async def get_task(self, task_id: str) -> Optional[Task]:
        """Get a specific task by ID"""
        try:
            response = await self._make_request("GET", f"/tasks/{task_id}")
            data = response
            return Task.model_validate(data)
        except Exception as e:
            logger.error(f"Error in get_task: {str(e)}\n{traceback.format_exc()}")
            return None

    async def create_task(self, task: TaskCreate) -> Optional[Task]:
        """Create a new task"""
        try:
            response = await self._make_request("POST", "/tasks", json=task.model_dump())
            data = response
            return Task.model_validate(data)
        except Exception as e:
            logger.error(f"Error in create_task: {str(e)}\n{traceback.format_exc()}")
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
            response = await self._make_request(
                "PUT",
                f"/tasks/{task_id}",
                json=update_data.model_dump(exclude_none=True)
            )
            data = response
            return Task.model_validate(data)
        except Exception as e:
            logger.error(f"Error in update_task: {str(e)}\n{traceback.format_exc()}")
            return None

    async def lock_task(self, task_id: str, worker_id: Optional[str] = None) -> bool:
        """Lock a task for processing"""
        try:
            worker = worker_id or self.worker_id
            logger.debug(f"Attempting to lock task {task_id} with worker {worker}")
            
            response = await self._make_request(
                "POST",
                f"/tasks/{task_id}/lock",
                json={"worker_id": worker}
            )
            
            data = response
            success = "Task locked successfully" in data.get("message", "")
            
            if success:
                logger.debug(f"Successfully locked task {task_id}")
            else:
                logger.warning(f"Failed to lock task {task_id}: {data.get('message', 'Unknown error')}")
            
            return success
        except Exception as e:
            logger.error(f"Error in lock_task for task {task_id}: {str(e)}\n{traceback.format_exc()}")
            return False

    async def unlock_task(self, task_id: str, worker_id: Optional[str] = None) -> bool:
        """Unlock a task"""
        try:
            response = await self._make_request(
                "POST",
                f"/tasks/{task_id}/unlock",
                json={"worker_id": worker_id or self.worker_id}
            )
            data = response
            return "Task unlocked successfully" in data.get("message", "")
        except Exception as e:
            logger.error(f"Error in unlock_task: {str(e)}\n{traceback.format_exc()}")
            return False

    async def send_heartbeat(self, task_id: str, worker_id: Optional[str] = None) -> bool:
        """Send heartbeat for a locked task"""
        try:
            response = await self._make_request(
                "POST",
                f"/tasks/{task_id}/heartbeat",
                json={"worker_id": worker_id or self.worker_id}
            )
            data = response
            return "Heartbeat updated successfully" in data.get("message", "")
        except Exception as e:
            logger.error(f"Error in send_heartbeat: {str(e)}\n{traceback.format_exc()}")
            return False

    async def close(self):
        """Close the client session"""
        await self.client.aclose()
