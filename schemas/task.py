from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class TaskPattern(str, Enum):
    BASIC = "basic"
    WORK_QUEUE = "work_queue"
    FANOUT = "fanout"
    ROUTING = "routing"


class TaskStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class TaskBase(BaseModel):
    title: str = Field(..., min_length=1, max_length=255, description="Task title")
    description: Optional[str] = Field(None, description="Task description")
    pattern: TaskPattern = Field(default=TaskPattern.BASIC, description="RabbitMQ pattern to use")


class TaskCreate(TaskBase):
    pass


class TaskUpdate(BaseModel):
    title: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    pattern: Optional[TaskPattern] = None


class TaskResponse(TaskBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    status: TaskStatus
    retry_count: int
    max_retries: int
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime] = None


class TaskListResponse(BaseModel):
    tasks: list[TaskResponse]
    total: int
    page: int
    page_size: int


class TaskStatusResponse(BaseModel):
    id: int
    status: TaskStatus
    retry_count: int
    error_message: Optional[str] = None
    updated_at: datetime
    completed_at: Optional[datetime] = None