from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from database import TaskStatus
from schemas import (
    TaskCreate,
    TaskUpdate,
    TaskResponse,
    TaskListResponse,
    TaskStatusResponse,
    TaskPattern,
)
from services import TaskService
from api.deps import get_task_service

router = APIRouter(prefix="/tasks", tags=["tasks"])


@router.post("", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def create_task(
    task_data: TaskCreate,
    service: TaskService = Depends(get_task_service),
):
    task = await service.create_task(task_data)
    return task


@router.get("", response_model=TaskListResponse)
async def list_tasks(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    status_filter: Optional[TaskStatus] = Query(None, alias="status", description="Filter by status"),
    service: TaskService = Depends(get_task_service),
):
    skip = (page - 1) * page_size
    tasks = await service.get_tasks(skip=skip, limit=page_size, status=status_filter)
    total = await service.count_tasks(status=status_filter)
    return TaskListResponse(
        tasks=tasks,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{task_id}", response_model=TaskResponse)
async def get_task(
    task_id: int,
    service: TaskService = Depends(get_task_service),
):
    task = await service.get_task(task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with id {task_id} not found",
        )
    return task


@router.get("/{task_id}/status", response_model=TaskStatusResponse)
async def get_task_status(
    task_id: int,
    service: TaskService = Depends(get_task_service),
):
    task_status = await service.get_task_status(task_id)
    if not task_status:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with id {task_id} not found",
        )
    return task_status


@router.patch("/{task_id}", response_model=TaskResponse)
async def update_task(
    task_id: int,
    task_data: TaskUpdate,
    service: TaskService = Depends(get_task_service),
):
    task = await service.update_task(task_id, task_data)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with id {task_id} not found",
        )
    return task


@router.delete("/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_task(
    task_id: int,
    service: TaskService = Depends(get_task_service),
):
    success = await service.delete_task(task_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with id {task_id} not found",
        )