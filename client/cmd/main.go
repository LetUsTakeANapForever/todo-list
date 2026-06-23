package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	chatpb "todo-client/proto/gen"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	address := "localhost:50051"
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := chatpb.NewChatClient(conn)
	ctx := context.Background()
	stream, err := client.Chat(ctx)
	if err != nil {
		log.Fatalf("failed to open chat stream: %v", err)
	}

	userID := uuid.New().String()
	currentRoom := "lobby"

	go recvLoop(stream)

	scanner := bufio.NewScanner(os.Stdin)
	printHelp()
	fmt.Printf("[INFO] userId=%s room=%s\n", userID, currentRoom)
	fmt.Printf("User ID %s joined the main lobby.\n", userID)

	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == "/quit" || line == "/exit" {
			break
		}
		if line == "/help" {
			printHelp()
			continue
		}
		if line == "/rooms" {
			resp, err := client.ListRooms(ctx, &chatpb.ListRoomsRequest{})
			if err != nil {
				log.Printf("list rooms failed: %v", err)
				continue
			}
			fmt.Printf("rooms: %v\n", resp.GetRoomIds())
			continue
		}
		if strings.HasPrefix(line, "/create ") {
			room := strings.TrimSpace(strings.TrimPrefix(line, "/create "))
			resp, err := client.CreateRoom(ctx, &chatpb.CreateRoomRequest{RoomId: room, UserId: userID})
			if err != nil {
				log.Printf("create room failed: %v", err)
				continue
			}
			fmt.Printf("%s: room %s\n", resp.GetMessage(), room)
			currentRoom = room
			continue
		}
		if strings.HasPrefix(line, "/join ") {
			room := strings.TrimSpace(strings.TrimPrefix(line, "/join "))
			resp, err := client.JoinRoom(ctx, &chatpb.JoinRoomRequest{RoomId: room, UserId: userID})
			if err != nil {
				log.Printf("join room failed: %v", err)
				continue
			}
			fmt.Printf("%s: %s\n", resp.GetMessage(), room)
			currentRoom = room
			continue
		}
		if strings.HasPrefix(line, "/leave ") {
			room := strings.TrimSpace(strings.TrimPrefix(line, "/leave "))
			resp, err := client.LeaveRoom(ctx, &chatpb.LeaveRoomRequest{RoomId: room, UserId: userID})
			if err != nil {
				log.Printf("leave room failed: %v", err)
				continue
			}
			fmt.Printf("%s: %s\n", resp.GetMessage(), room)
			continue
		}
		if strings.HasPrefix(line, "/room ") {
			currentRoom = strings.TrimSpace(strings.TrimPrefix(line, "/room "))
			fmt.Printf("current room set to %s\n", currentRoom)
			continue
		}
		if strings.HasPrefix(line, "/typing ") {
			parts := strings.Fields(line)
			if len(parts) < 3 {
				fmt.Println("usage: /typing <room> <on|off>")
				continue
			}
			sendTyping(stream, userID, parts[1], parts[2] == "on")
			continue
		}
		if strings.HasPrefix(line, "/msg ") {
			payload := strings.TrimSpace(strings.TrimPrefix(line, "/msg "))
			room, text, ok := splitRoomMessage(payload, currentRoom)
			if !ok {
				fmt.Println("usage: /msg [room] <text>")
				continue
			}
			if err := sendMessage(stream, userID, room, text); err != nil {
				log.Fatalf("send message failed: %v", err)
			}
			continue
		}

		room, text, ok := splitRoomMessage(line, currentRoom)
		if !ok {
			fmt.Println("usage: /msg [room] <text> or commands from /help")
			continue
		}
		if err := sendMessage(stream, userID, room, text); err != nil {
			log.Fatalf("send message failed: %v", err)
		}
	}

	_ = stream.CloseSend()
}

func recvLoop(stream chatpb.Chat_ChatClient) {
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Printf("recv error: %v", err)
			return
		}

		switch ev := resp.GetEvent().(type) {
		case *chatpb.ChatResponse_NewMessage:
			msg := ev.NewMessage
			fmt.Printf("\nNEW MESSAGE | room=%s sender=%s text=%s time=%s\n",
				msg.GetRoomId(), msg.GetSenderId(), msg.GetText(), msg.GetTimestamp())
		case *chatpb.ChatResponse_UserTyping:
			typing := ev.UserTyping
			fmt.Printf("\nTYPING | room=%s user=%s isTyping=%v\n",
				typing.GetRoomId(), typing.GetUserId(), typing.GetIsTyping())
		case *chatpb.ChatResponse_SystemNotification:
			sys := ev.SystemNotification
			fmt.Printf("\nSYSTEM | type=%s message=%s time=%s\n",
				sys.GetType(), sys.GetMessage(), sys.GetTimestamp())
		default:
			fmt.Println("\nunknown event")
		}
		fmt.Print("> ")
	}
}

func sendMessage(stream chatpb.Chat_ChatClient, userID, room, text string) error {
	return stream.Send(&chatpb.ChatRequest{
		UserId: userID,
		Payload: &chatpb.ChatRequest_SendMessage{
			SendMessage: &chatpb.SendMessageRequest{
				RoomId: room,
				Text:   text,
			},
		},
	})
}

func sendTyping(stream chatpb.Chat_ChatClient, userID, room string, isTyping bool) {
	err := stream.Send(&chatpb.ChatRequest{
		UserId: userID,
		Payload: &chatpb.ChatRequest_TypingStatus{
			TypingStatus: &chatpb.TypingRequest{
				RoomId:   room,
				IsTyping: isTyping,
			},
		},
	})
	if err != nil {
		log.Printf("send typing failed: %v", err)
	}
}

func splitRoomMessage(input, fallbackRoom string) (room, text string, ok bool) {
	fields := strings.Fields(input)
	if len(fields) == 0 {
		return "", "", false
	}
	if len(fields) == 1 {
		return fallbackRoom, fields[0], true
	}
	if strings.HasPrefix(fields[0], "#") || strings.HasPrefix(fields[0], "@") {
		room = strings.TrimPrefix(strings.TrimPrefix(fields[0], "#"), "@")
		text = strings.Join(fields[1:], " ")
		return room, text, true
	}
	return fallbackRoom, input, true
}

func printHelp() {
	fmt.Println("commands:")
	fmt.Println("  /help")
	fmt.Println("  /rooms")
	fmt.Println("  /create <room>")
	fmt.Println("  /join <room>")
	fmt.Println("  /leave <room>")
	fmt.Println("  /room <room>")
	fmt.Println("  /typing <room> <on|off>")
	fmt.Println("  /msg [room] <text>")
	fmt.Println("  /quit")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
