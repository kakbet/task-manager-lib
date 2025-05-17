"""
Task worker implementation for handling task execution
"""
import asyncio
import logging
import uuid
from typing import Dict, Optional, Callable, Awaitable, Any
import httpx

from .client import TaskManagerClient
from .models import Task

logger = logging.getLogger(__name__)

class TaskWorker:
    def __init__(
        self,
        max_concurrent_tasks: int = 10,
        poll_interval: float = 1.0,
        client: Optional[TaskManagerClient] = None,
        worker_id: Optional[str] = None,
        api_url: str = "http://localhost:8000",
        heartbeat_interval: int = 30,  # saniye
        lock_timeout: int = 1800,  # 30 dakika
    ):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.poll_interval = poll_interval
        self.client = client or TaskManagerClient()
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.handlers: Dict[str, Callable[[Task], Awaitable[Any]]] = {}
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:8]}"
        self.api_url = api_url
        self.heartbeat_interval = heartbeat_interval
        self.lock_timeout = lock_timeout
        self._current_task_id: Optional[str] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._stop_heartbeat = asyncio.Event()

    def register_handler(self, task_type: str, handler: Callable[[Task], Awaitable[Any]]) -> None:
        """Register a handler for a specific task type"""
        self.handlers[task_type] = handler
        logger.info(f"Registered handler for task type: {task_type}")

    async def get_next_task(self) -> Optional[Task]:
        """Get next available task from the task manager"""
        try:
            tasks = await self.client.list_tasks(status="queued", limit=1)
            return tasks[0] if tasks else None
        except Exception as e:
            logger.error(f"Error getting next task: {e}")
            return None

    async def lock_task(self, task_id: str) -> bool:
        """Try to acquire lock for a task"""
        try:
            return await self.client.lock_task(task_id, worker_id=self.worker_id)
        except Exception as e:
            logger.error(f"Error locking task {task_id}: {e}")
            return False

    async def _send_heartbeat(self):
        """Periyodik olarak heartbeat gönder"""
        if not self._current_task_id:
            return

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.api_url}/tasks/{self._current_task_id}/heartbeat",
                    json={"worker_id": self.worker_id}
                )
                response.raise_for_status()
                logger.debug(f"Heartbeat sent for task {self._current_task_id}")
        except Exception as e:
            logger.error(f"Error sending heartbeat: {str(e)}")

    async def _run_heartbeat(self):
        """Heartbeat loop'unu çalıştır"""
        while not self._stop_heartbeat.is_set():
            await self._send_heartbeat()
            try:
                await asyncio.wait_for(
                    self._stop_heartbeat.wait(),
                    timeout=self.heartbeat_interval
                )
            except asyncio.TimeoutError:
                continue

    async def process_task(self, task: Task) -> None:
        """Process a single task"""
        self._current_task_id = task.id
        
        # Heartbeat task'ını başlat
        self._stop_heartbeat.clear()
        self._heartbeat_task = asyncio.create_task(self._run_heartbeat())

        try:
            # Task'ı kilitle
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.api_url}/tasks/{task.id}/lock",
                    json={"worker_id": self.worker_id}
                )
                response.raise_for_status()

            # Task'ı işle
            handler = self.handlers.get(task.type)
            if not handler:
                logger.error(f"No handler registered for task type: {task.type}")
                await self.client.update_task(task.id, status="failed", error="No handler registered")
                return

            await self.client.update_task(task.id, status="running")
            
            try:
                result = await handler(task)
                await self.client.update_task(task.id, status="completed", result=result)
            except Exception as e:
                logger.error(f"Error processing task {task.id}: {e}")
                await self.client.update_task(task.id, status="failed", error=str(e))
                
        except Exception as e:
            logger.error(f"Error in process_task for {task.id}: {e}")
        finally:
            # Heartbeat'i durdur
            self._stop_heartbeat.set()
            if self._heartbeat_task:
                await self._heartbeat_task
            self._current_task_id = None

            # Task'ı unlock et
            try:
                async with httpx.AsyncClient() as client:
                    await client.post(
                        f"{self.api_url}/tasks/{task.id}/unlock",
                        json={"worker_id": self.worker_id}
                    )
            except Exception as e:
                logger.error(f"Error unlocking task {task.id}: {str(e)}")

    async def start(self) -> None:
        """Start the worker loop"""
        logger.info("Starting worker...")
        
        try:
            while True:
                try:
                    # Check if we can take more tasks
                    if len(self.active_tasks) >= self.max_concurrent_tasks:
                        await asyncio.sleep(self.poll_interval)
                        continue
                    
                    # Get next task
                    task = await self.get_next_task()
                    if task:
                        # Check if we have a handler for this task type
                        if task.type not in self.handlers:
                            logger.info(f"Skipping task {task.id} - no handler for type: {task.type}")
                            await asyncio.sleep(self.poll_interval)
                            continue
                            
                        # Try to acquire lock
                        if await self.lock_task(task.id):
                            logger.info(f"Starting task {task.id}")
                            # Create task
                            task_obj = asyncio.create_task(self.process_task(task))
                            self.active_tasks[task.id] = task_obj
                        else:
                            logger.warning(f"Could not acquire lock for task {task.id}")
                    else:
                        await asyncio.sleep(self.poll_interval)
                    
                    # Clean up completed tasks
                    done_tasks = []
                    for task_id, task_obj in self.active_tasks.items():
                        if task_obj.done():
                            done_tasks.append(task_id)
                    
                    for task_id in done_tasks:
                        del self.active_tasks[task_id]
                    
                except Exception as e:
                    logger.error(f"Error in worker loop: {e}")
                    await asyncio.sleep(self.poll_interval)
        finally:
            # Clean up on exit
            if self.client:
                await self.client.close()
