from typing import Generator
from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session
from database import get_db
from services import TaskService


def get_task_service(db: Session = Depends(get_db)) -> TaskService:
    return TaskService(db)