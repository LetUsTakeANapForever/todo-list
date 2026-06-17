package todo_service

import (
	"sync"
	todoPb "todo-server/proto/gen"
)

type Server struct {
	todoPb.UnimplementedTodoServiceServer

	// in-memory store
	mu    sync.Mutex
	tasks map[string]*todoPb.Task
}
