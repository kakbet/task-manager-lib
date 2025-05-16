"""
Task worker implementation for handling task execution
"""
import asyncio
import logging
import uuid
from typing import Dict, Optional, Callable, Awaitable, Any

from .client import TaskManagerClient
from .models import Task

logger = logging.getLogger(__name__)

class TaskWorker:
    def __init__(
        self,
        max_concurrent_tasks: int = 10,
        poll_interval: float = 1.0,
        client: Optional[TaskManagerClient] = None,
        worker_id: Optional[str] = None
    ):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.poll_interval = poll_interval
        self.client = client or TaskManagerClient()
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.handlers: Dict[str, Callable[[Task], Awaitable[Any]]] = {}
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:8]}"

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

    async def process_task(self, task: Task) -> None:
        """Process a single task"""
        try:
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
            try:
                await self.client.unlock_task(task.id)
            except Exception as e:
                logger.error(f"Error unlocking task {task.id}: {e}")

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
