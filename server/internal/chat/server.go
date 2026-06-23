package chat

import (
	chatPb "todo-server/proto/gen"
)

type Server struct {
	chatPb.UnimplementedChatServer
}
