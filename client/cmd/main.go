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
	joinedRooms := map[string]struct{}{}

	go recvLoop(stream)
	// subscribe to lobby events
	go recvLobby(client, ctx, userID)

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
			joinedRooms[room] = struct{}{}
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
			if resp.GetJoined() || resp.GetCreated() {
				joinedRooms[room] = struct{}{}
				currentRoom = room
			}
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
			delete(joinedRooms, room)
			if currentRoom == room {
				currentRoom = "lobby"
			}
			continue
		}
		if strings.HasPrefix(line, "/room ") {
			room := strings.TrimSpace(strings.TrimPrefix(line, "/room "))
			if room == "lobby" {
				currentRoom = "lobby"
				fmt.Println("current room set to lobby")
				continue
			}
			if _, ok := joinedRooms[room]; !ok {
				fmt.Printf("room %s is not joined; use /join or /create first\n", room)
				continue
			}
			currentRoom = room
			fmt.Printf("current room set to %s\n", currentRoom)
			continue
		}
		if strings.HasPrefix(line, "/typing ") {
			parts := strings.Fields(line)
			if len(parts) < 3 {
				fmt.Println("usage: /typing <room> <on|off>")
				continue
			}
			if parts[1] == "lobby" {
				fmt.Println("lobby is not a chat room")
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
			if room == "lobby" {
				fmt.Println("join or create a room before sending messages")
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
		if room == "lobby" {
			fmt.Println("join or create a room before sending messages")
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

func recvLobby(client chatpb.ChatClient, ctx context.Context, userID string) {
	stream, err := client.Lobby(ctx, &chatpb.SubscribeLobbyRequest{UserId: userID})
	if err != nil {
		log.Printf("failed to subscribe to lobby: %v", err)
		return
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Printf("lobby recv error: %v", err)
			return
		}
		switch ev := resp.GetEvent().(type) {
		case *chatpb.LobbyResponse_RoomList:
			rl := ev.RoomList
			fmt.Printf("\nLOBBY ROOMS: %v\n", rl.GetRoomIds())
		case *chatpb.LobbyResponse_UserEvent:
			u := ev.UserEvent
			fmt.Printf("\nLOBBY USER EVENT: user=%s room=%s action=%s\n", u.GetUserId(), u.GetRoomId(), u.GetAction())
		case *chatpb.LobbyResponse_SystemNotification:
			sn := ev.SystemNotification
			fmt.Printf("\nLOBBY SYSTEM: type=%s message=%s\n", sn.GetType(), sn.GetMessage())
		default:
			fmt.Println("\nunknown lobby event")
		}
		fmt.Print("> ")
	}
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
