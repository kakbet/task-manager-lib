"""
Task worker implementation for handling task execution
"""
import asyncio
import logging
import uuid
import traceback
from typing import Dict, Optional, Callable, Awaitable, Any
import httpx
from datetime import datetime, timedelta

from .client import TaskManagerClient
from .models import Task

# Log seviyesini DEBUG'a çek
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class TaskWorker:
    def __init__(
        self,
        max_concurrent_tasks: int = 10,
        poll_interval: float = 0.5,  # 0.5 saniyeye düşürdük
        client: Optional[TaskManagerClient] = None,
        worker_id: Optional[str] = None,
        api_url: str = "http://127.0.0.1:8001",  # Using IP address instead of localhost
        heartbeat_interval: int = 30,  # saniye
        lock_timeout: int = 1800,  # 30 dakika
        connection_retry_interval: int = 5,  # saniye
        max_connection_retries: int = 3
    ):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.poll_interval = poll_interval
        self.worker_id = worker_id or f"worker_{uuid.uuid4().hex[:8]}"
        self.connection_retry_interval = connection_retry_interval
        self.max_connection_retries = max_connection_retries
        
        # Client'ı oluştur ve worker ID'yi ayarla
        self.client = client or TaskManagerClient(api_url)
        self.client.set_worker_id(self.worker_id)
        
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.handlers: Dict[str, Callable[[Task], Awaitable[Any]]] = {}
        self.heartbeat_interval = heartbeat_interval
        self.lock_timeout = lock_timeout
        self._current_task_id: Optional[str] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._stop_heartbeat = asyncio.Event()
        self._last_connection_error = None
        self._connection_error_count = 0
        self._last_task_check = None
        self._no_task_count = 0

    async def get_next_task(self) -> Optional[Task]:
        """Get next available task from the task manager"""
        try:
            current_time = datetime.utcnow()
            
            # Log if we haven't seen a task in a while
            if self._last_task_check:
                time_since_last_check = (current_time - self._last_task_check).total_seconds()
                if time_since_last_check > 60:  # 1 dakikadan uzun süredir task yok
                    logger.warning(f"No tasks found for {time_since_last_check:.0f} seconds")
                    self._no_task_count += 1
                    if self._no_task_count % 5 == 0:  # Her 5 uyarıda bir detaylı log
                        logger.info(f"Worker state: Active tasks: {len(self.active_tasks)}, Handlers: {list(self.handlers.keys())}")
            
            tasks = await self.client.list_tasks(status="queued", limit=1)
            self._last_task_check = current_time
            
            if tasks:
                self._no_task_count = 0
                logger.debug(f"Found queued task: {tasks[0].id} of type {tasks[0].type}")
                return tasks[0]
            
            return None
            
        except Exception as e:
            self._last_connection_error = e
            self._connection_error_count += 1
            
            if self._connection_error_count >= self.max_connection_retries:
                logger.error(f"Failed to connect to task manager after {self._connection_error_count} attempts: {e}")
                await asyncio.sleep(self.connection_retry_interval * 2)  # Daha uzun bekle
            else:
                logger.warning(f"Error getting next task (attempt {self._connection_error_count}): {e}")
                await asyncio.sleep(self.connection_retry_interval)
            
            return None

    def register_handler(self, task_type: str, handler: Callable[[Task], Awaitable[Any]]) -> None:
        """Register a handler for a specific task type"""
        self.handlers[task_type] = handler
        logger.info(f"Registered handler for task type: {task_type}")

    async def lock_task(self, task_id: str) -> bool:
        """Try to acquire lock for a task"""
        try:
            success = await self.client.lock_task(task_id)
            if success:
                logger.debug(f"Successfully locked task {task_id}")
                self._last_connection_error = None
                self._connection_error_count = 0
            else:
                logger.warning(f"Failed to lock task {task_id}")
            return success
        except Exception as e:
            self._last_connection_error = e
            self._connection_error_count += 1
            logger.error(f"Error locking task {task_id}: {e}")
            return False

    async def _send_heartbeat(self):
        """Periyodik olarak heartbeat gönder"""
        if not self._current_task_id:
            return

        try:
            success = await self.client.send_heartbeat(self._current_task_id)
            if success:
                logger.debug(f"Heartbeat sent for task {self._current_task_id}")
                self._last_connection_error = None
                self._connection_error_count = 0
            else:
                logger.warning(f"Failed to send heartbeat for task {self._current_task_id}")
        except Exception as e:
            self._last_connection_error = e
            self._connection_error_count += 1
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
            # Task handler'ı kontrol et
            handler = self.handlers.get(task.type)
            if not handler:
                logger.error(f"No handler registered for task type: {task.type}")
                await self.client.update_task(task.id, status="failed", error="No handler registered")
                return

            # Task'ı işle
            try:
                result = await handler(task)
                await self.client.update_task(task.id, status="completed", result=result)
                logger.info(f"Task {task.id} completed successfully")
            except Exception as e:
                error_msg = f"Error processing task: {str(e)}"
                logger.error(f"Task {task.id} failed: {error_msg}")
                await self.client.update_task(task.id, status="failed", error=error_msg)
                
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
                success = await self.client.unlock_task(task.id)
                if success:
                    logger.debug(f"Successfully unlocked task {task.id}")
                else:
                    logger.warning(f"Failed to unlock task {task.id}")
            except Exception as e:
                logger.error(f"Error unlocking task {task.id}: {str(e)}")

    async def start(self) -> None:
        """Start the worker loop"""
        logger.info(f"Starting worker {self.worker_id}...")
        
        try:
            while True:
                try:
                    # Check if we can take more tasks
                    if len(self.active_tasks) >= self.max_concurrent_tasks:
                        await asyncio.sleep(self.poll_interval)
                        continue
                    
                    # Get next task
                    task = await self.get_next_task()
                    if not task:
                        await asyncio.sleep(self.poll_interval)
                        continue

                    # Check if we have a handler for this task type
                    if task.type not in self.handlers:
                        logger.info(f"Skipping task {task.id} - no handler for type: {task.type}")
                        await asyncio.sleep(self.poll_interval)
                        continue
                        
                    # Try to acquire lock
                    if await self.lock_task(task.id):
                        logger.info(f"Starting task {task.id} of type {task.type}")
                        # Create task
                        task_obj = asyncio.create_task(self.process_task(task))
                        self.active_tasks[task.id] = task_obj
                    else:
                        logger.warning(f"Could not acquire lock for task {task.id}")
                    
                    # Clean up completed tasks
                    done_tasks = []
                    for task_id, task_obj in self.active_tasks.items():
                        if task_obj.done():
                            done_tasks.append(task_id)
                            # Check for exceptions
                            if task_obj.exception():
                                logger.error(f"Task {task_id} failed with error: {task_obj.exception()}")
                    
                    for task_id in done_tasks:
                        del self.active_tasks[task_id]
                    
                except Exception as e:
                    logger.error(f"Error in worker loop: {str(e)}")
                    await asyncio.sleep(self.poll_interval)
        finally:
            # Clean up on exit
            if self.client:
                await self.client.close()
