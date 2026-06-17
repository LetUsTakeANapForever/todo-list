package todo_service

import (
	"context"
	"strconv"
	"time"
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

	now := time.Now().UTC().Format(time.RFC3339) // e.g. 2006-01-02T15:04:05Z07:00
	id := strconv.FormatInt(time.Now().UnixNano(), 10)

	task := &todoPb.Task{
		Id:          id,
		UserId:      req.GetUserId(),
		Title:       req.GetTitle(),
		Description: req.GetDescription(),
		Priority:    req.GetPriority(),
		IsComplete:  false,
		DueDate:     req.GetDueDate(),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	s.mu.Lock()
	if s.tasks == nil { // checks if task storage map has been created yet
		s.tasks = make(map[string]*todoPb.Task)
	}
	s.tasks[id] = task
	s.mu.Unlock()

	return &todoPb.CreateTaskResponse{
		Status: "Success",
		TaskId: id,
	}, nil
}
