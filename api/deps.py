from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from database import get_db
from services import TaskService


async def get_task_service(db: AsyncSession = Depends(get_db)) -> TaskService:
    return TaskService(db)