package todo_service

import (
	"context"
	"strconv"
	"time"
	"todo-server/internal/utils"
	todoPb "todo-server/proto/gen"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) CreateTask(ctx context.Context, req *todoPb.CreateTaskRequest) (*todoPb.CreateTaskResponse, error) {
	// Validate inputs
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "invalid request")
	}
	if req.GetTitle() == "" {
		return nil, status.Error(codes.InvalidArgument, "title is required")
	}
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "userId is required")
	}
	if req.GetPriority() == "" {
		return nil, status.Error(codes.InvalidArgument, "priority is required")
	}

	// allow empty dueDate (optional) — if provided, parse and normalize to RFC3339
	var parsedDueDate time.Time
	var err error
	if req.GetDueDate() != "" {
		parsedDueDate, err = utils.ParseDueDate(req.GetDueDate())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid dueDate format: %v", err)
		}
	}

	now := time.Now().UTC().Format(time.RFC3339) // e.g. 2006-01-02T15:04:05Z07:00
	id := strconv.FormatInt(time.Now().UnixNano(), 10)

	dueDateStr := ""
	if !parsedDueDate.IsZero() {
		dueDateStr = parsedDueDate.UTC().Format(time.RFC3339)
	}

	task := &todoPb.Task{
		Id:          id,
		UserId:      req.GetUserId(),
		Title:       req.GetTitle(),
		Description: req.GetDescription(),
		Priority:    req.GetPriority(),
		IsComplete:  false,
		DueDate:     dueDateStr,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tasks == nil { // checks if task storage map has been created yet
		s.tasks = make(map[string]*todoPb.Task)
	}
	s.tasks[id] = task

	return &todoPb.CreateTaskResponse{
		Status: "Success",
		TaskId: id,
	}, nil
}

func (s *Server) GetTasks(ctx context.Context, req *todoPb.GetTasksRequest) (*todoPb.GetTasksResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "userId is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var tasks []*todoPb.Task
	if s.tasks != nil {
		for _, task := range s.tasks {
			if task.GetUserId() == req.GetUserId() {
				tasks = append(tasks, task)
			}
		}
	}

	return &todoPb.GetTasksResponse{
		Status: "Success",
		Tasks:  tasks,
	}, nil
}

func (s *Server) GetTaskById(ctx context.Context, req *todoPb.GetTaskByIdRequest) (*todoPb.GetTaskByIdResponse, error) {
	if req.GetTaskId() == "" {
		return nil, status.Error(codes.InvalidArgument, "taskId is required")
	}

	if s.tasks != nil {
		for _, task := range s.tasks {
			if task.GetId() == req.GetTaskId() {
				return &todoPb.GetTaskByIdResponse{
					Status: "success",
					Task:   task,
				}, nil
			}
		}
	}

	return nil, status.Errorf(codes.NotFound, "taskId:%s not found\n", req.GetTaskId())
}

func (s *Server) UpdateTask(ctx context.Context, req *todoPb.UpdateTaskRequest) (*todoPb.UpdateTaskResponse, error) {
	var parsedDueDate time.Time
	var err error
	if req.GetDueDate() != "" {
		parsedDueDate, err = utils.ParseDueDate(req.GetDueDate())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid dueDate format: %v", err)
		}
	}

	var task *todoPb.Task
	if s.tasks != nil {
		for _, t := range s.tasks {
			if t.GetId() == req.GetTaskId() {
				task = t
			}
		}
	}

	if task == nil {
		return nil, status.Errorf(codes.NotFound, "taskId: %s not found", req.GetTaskId())
	}

	if req.GetTitle() != "" {
		task.Title = req.GetTitle()
	}
	if req.GetDescription() != "" {
		task.Title = req.GetDescription()
	}
	if req.GetPriority() != "" {
		task.Priority = req.GetPriority()
	}
	if req.GetDueDate() != "" {
		task.DueDate = parsedDueDate.UTC().Format(time.RFC3339)
	}
	return &todoPb.UpdateTaskResponse{
		Status: "success",
		Task:   task,
	}, nil
}

func (s *Server) DeleteTask(ctx context.Context, req *todoPb.DeleteTaskRequest) (*todoPb.DeleteTaskResponse, error) {
	if req.GetTaskId() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "taskId is required")
	}

	var task *todoPb.Task
	if s.tasks != nil {
		for _, t := range s.tasks {
			if req.GetTaskId() == t.GetId() {
				task = t
			}
		}
	}

	if task == nil {
		return nil, status.Errorf(codes.NotFound, "taskId: %s not found", req.GetTaskId())
	}

	s.tasks[req.GetTaskId()] = nil

	return &todoPb.DeleteTaskResponse{
		Status: "Sucess",
	}, nil
}
