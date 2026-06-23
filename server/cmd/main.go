package main

import (
	"log"
	"net"

	chat "todo-server/internal/chat"
	todo "todo-server/internal/todo"
	pb "todo-server/proto/gen"

	"google.golang.org/grpc"
)

func main() {
	port := ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalln("Failed to listen:", err)
	}

	grpcServer := grpc.NewServer()

	pb.RegisterChatServer(grpcServer, &chat.Server{})
	pb.RegisterTodoServiceServer(grpcServer, &todo.Server{})

	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalln("Failed to start server:", err)
	}
	log.Println("Server listening on port:", port)
}
