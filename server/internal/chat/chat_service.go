package chat

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"
	chatPb "todo-server/proto/gen"
)

const roomHistoryLimit = 50
const lobbyRoomName = "lobby"

// Client represents a connected bidi-stream participant
type Client struct {
	stream chatPb.Chat_ChatServer
	send   chan *chatPb.ChatResponse
	userId string
	rooms  map[string]struct{}
}

type roomState struct {
	members map[*Client]struct{}
	history []*chatPb.ChatResponse
}

// Hub keeps track of clients per room and broadcasts events
type Hub struct {
	mu        sync.RWMutex
	rooms     map[string]*roomState
	users     map[string]map[string]struct{}
	lobbySubs map[chan *chatPb.LobbyResponse]struct{}
}

var globalHub = &Hub{
	rooms:     make(map[string]*roomState),
	users:     make(map[string]map[string]struct{}),
	lobbySubs: make(map[chan *chatPb.LobbyResponse]struct{}),
}

func (h *Hub) addLobbySubscriber(ch chan *chatPb.LobbyResponse) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.lobbySubs == nil {
		h.lobbySubs = make(map[chan *chatPb.LobbyResponse]struct{})
	}
	h.lobbySubs[ch] = struct{}{}
}

func (h *Hub) removeLobbySubscriber(ch chan *chatPb.LobbyResponse) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.lobbySubs, ch)
}

func (h *Hub) broadcastLobby(resp *chatPb.LobbyResponse) {
	if resp == nil {
		return
	}
	h.mu.RLock()
	subs := make([]chan *chatPb.LobbyResponse, 0, len(h.lobbySubs))
	for ch := range h.lobbySubs {
		subs = append(subs, ch)
	}
	h.mu.RUnlock()
	for _, ch := range subs {
		select {
		case ch <- resp:
		default:
		}
	}
}

func (h *Hub) addClientToRoom(room string, c *Client) (joined bool) {
	if room == "" {
		return false
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	state := h.rooms[room]
	if state == nil {
		state = &roomState{members: make(map[*Client]struct{})}
		h.rooms[room] = state
	}

	if _, ok := state.members[c]; ok {
		return false
	}

	state.members[c] = struct{}{}
	c.rooms[room] = struct{}{}
	return true
}

func (h *Hub) ensureRoom(room string) (created bool) {
	if room == "" {
		return false
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[room] == nil {
		h.rooms[room] = &roomState{members: make(map[*Client]struct{})}
		return true
	}
	return false
}

func (h *Hub) addUserToRoom(room, user string) (joined bool) {
	if room == "" || user == "" {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.rooms[room] == nil {
		h.rooms[room] = &roomState{members: make(map[*Client]struct{})}
	}
	if h.users[user] == nil {
		h.users[user] = make(map[string]struct{})
	}
	if _, ok := h.users[user][room]; ok {
		return false
	}
	h.users[user][room] = struct{}{}
	return true
}

func (h *Hub) removeUserFromRoom(room, user string) (left bool) {
	if room == "" || user == "" {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	rooms := h.users[user]
	if rooms == nil {
		return false
	}
	if _, ok := rooms[room]; !ok {
		return false
	}
	delete(rooms, room)
	if len(rooms) == 0 {
		delete(h.users, user)
	}
	return true
}

func (h *Hub) userRooms(user string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	rooms := h.users[user]
	out := make([]string, 0, len(rooms))
	for room := range rooms {
		out = append(out, room)
	}
	return out
}

func (h *Hub) roomExists(room string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.rooms[room] != nil
}

func (h *Hub) roomNames() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.rooms))
	for room := range h.rooms {
		out = append(out, room)
	}
	return out
}

func (h *Hub) removeClientFromRoom(room string, c *Client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	state := h.rooms[room]
	if state == nil {
		return false
	}
	if _, ok := state.members[c]; !ok {
		return false
	}
	delete(state.members, c)
	delete(c.rooms, room)
	if len(state.members) == 0 {
		delete(h.rooms, room)
	}
	return true
}

func (h *Hub) roomHistory(room string) []*chatPb.ChatResponse {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if state := h.rooms[room]; state != nil {
		out := make([]*chatPb.ChatResponse, len(state.history))
		copy(out, state.history)
		return out
	}
	return nil
}

func (h *Hub) appendHistory(room string, response *chatPb.ChatResponse) {
	if room == "" || response == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	state := h.rooms[room]
	if state == nil {
		state = &roomState{members: make(map[*Client]struct{})}
		h.rooms[room] = state
	}

	state.history = append(state.history, response)
	if len(state.history) > roomHistoryLimit {
		state.history = state.history[len(state.history)-roomHistoryLimit:]
	}
}

func (h *Hub) removeClient(c *Client) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var emptied []string
	for room := range c.rooms {
		if state, ok := h.rooms[room]; ok {
			delete(state.members, c)
			if len(state.members) == 0 {
				delete(h.rooms, room)
				emptied = append(emptied, room)
			}
		}
	}
	return emptied
}

func (h *Hub) broadcast(room string, response *chatPb.ChatResponse) {
	h.mu.RLock()
	state := h.rooms[room]
	if state == nil {
		h.mu.RUnlock()
		return
	}
	clients := make([]*Client, 0, len(state.members))
	for c := range state.members {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, c := range clients {
		select {
		case c.send <- response:
		default:
		}
	}
}

func systemNotification(room, kind, message string) *chatPb.ChatResponse {
	return &chatPb.ChatResponse{
		Event: &chatPb.ChatResponse_SystemNotification{
			SystemNotification: &chatPb.SystemNotification{
				Type:      kind,
				Message:   message,
				Timestamp: time.Now().Format(time.RFC3339),
			},
		},
	}
}

func roomResponse(room string, created, joined, left bool, message string) *chatPb.RoomResponse {
	return &chatPb.RoomResponse{
		RoomId:  room,
		Created: created,
		Joined:  joined,
		Left:    left,
		Message: message,
	}
}

func (s *Server) CreateRoom(ctx context.Context, req *chatPb.CreateRoomRequest) (*chatPb.RoomResponse, error) {
	if req.GetRoomId() == lobbyRoomName {
		return roomResponse(req.GetRoomId(), false, false, false, "lobby is reserved"), nil
	}
	created := globalHub.ensureRoom(req.GetRoomId())
	message := "room already exists"
	if created {
		message = "room created"
		// notify lobby subscribers of new room list
		globalHub.broadcastLobby(&chatPb.LobbyResponse{
			Event: &chatPb.LobbyResponse_RoomList{
				RoomList: &chatPb.RoomList{RoomIds: globalHub.roomNames()},
			},
		})
	}
	return roomResponse(req.GetRoomId(), created, false, false, message), nil
}

func (s *Server) JoinRoom(ctx context.Context, req *chatPb.JoinRoomRequest) (*chatPb.RoomResponse, error) {
	if req.GetRoomId() == lobbyRoomName {
		return roomResponse(req.GetRoomId(), false, false, false, "lobby is reserved"), nil
	}
	created := globalHub.ensureRoom(req.GetRoomId())
	joined := globalHub.addUserToRoom(req.GetRoomId(), req.GetUserId())
	message := "joined room"
	if !joined {
		message = "already joined or invalid room"
	}
	if created {
		message = "room created and joined"
	}
	// notify lobby subscribers that a user joined this room
	if joined {
		globalHub.broadcastLobby(&chatPb.LobbyResponse{
			Event: &chatPb.LobbyResponse_UserEvent{
				UserEvent: &chatPb.LobbyUserEvent{
					UserId: req.GetUserId(),
					RoomId: req.GetRoomId(),
					Action: "joined",
				},
			},
		})
	}
	return roomResponse(req.GetRoomId(), created, joined, false, message), nil
}

func (s *Server) LeaveRoom(ctx context.Context, req *chatPb.LeaveRoomRequest) (*chatPb.RoomResponse, error) {
	if req.GetRoomId() == lobbyRoomName {
		return roomResponse(req.GetRoomId(), false, false, false, "lobby is reserved"), nil
	}
	left := globalHub.removeUserFromRoom(req.GetRoomId(), req.GetUserId())
	if left {
		globalHub.broadcastLobby(&chatPb.LobbyResponse{
			Event: &chatPb.LobbyResponse_UserEvent{
				UserEvent: &chatPb.LobbyUserEvent{
					UserId: req.GetUserId(),
					RoomId: req.GetRoomId(),
					Action: "left",
				},
			},
		})
	}
	return roomResponse(req.GetRoomId(), false, false, left, "left room"), nil
}

func (s *Server) ListRooms(ctx context.Context, req *chatPb.ListRoomsRequest) (*chatPb.ListRoomsResponse, error) {
	return &chatPb.ListRoomsResponse{RoomIds: globalHub.roomNames()}, nil
}

// Lobby is a server-streaming RPC that sends lobby-level events (room list, user join/leave, system notifications)
func (s *Server) Lobby(req *chatPb.SubscribeLobbyRequest, stream chatPb.Chat_LobbyServer) error {
	ctx := stream.Context()
	ch := make(chan *chatPb.LobbyResponse, 8)
	globalHub.addLobbySubscriber(ch)
	defer func() {
		globalHub.removeLobbySubscriber(ch)
		close(ch)
	}()

	// send initial room list immediately
	initMsg := &chatPb.LobbyResponse{
		Event: &chatPb.LobbyResponse_RoomList{RoomList: &chatPb.RoomList{RoomIds: globalHub.roomNames()}},
	}
	if err := stream.Send(initMsg); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (s *Server) Chat(stream chatPb.Chat_ChatServer) error {
	ctx := stream.Context()
	c := &Client{
		stream: stream,
		send:   make(chan *chatPb.ChatResponse, 16),
		rooms:  make(map[string]struct{}),
	}

	// Sender goroutine: writes outbound messages to the stream
	var sendErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-c.send:
				if !ok {
					return
				}
				if err := stream.Send(msg); err != nil {
					sendErr = err
					return
				}
			}
		}
	}()

	// Recieve loop
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Cleanup
			globalHub.removeClient(c)
			close(c.send)
			wg.Wait()
			return err
		}

		// Update last-known user id if provided
		if req.GetUserId() != "" {
			c.userId = req.GetUserId()
			for _, room := range globalHub.userRooms(c.userId) {
				globalHub.addClientToRoom(room, c)
			}
		}

		switch p := req.GetPayload().(type) {
		case *chatPb.ChatRequest_SendMessage:
			sendMessage := p.SendMessage
			if sendMessage == nil {
				continue
			}
			if sendMessage.GetRoomId() == "" || sendMessage.GetRoomId() == lobbyRoomName {
				continue
			}
			// Register client to room so they receive future broadcasts.
			if globalHub.addClientToRoom(sendMessage.GetRoomId(), c) {
				globalHub.broadcast(sendMessage.GetRoomId(), systemNotification(
					sendMessage.GetRoomId(),
					"joined",
					fmt.Sprintf("%s joined room %s", c.userId, sendMessage.GetRoomId()),
				))
				for _, past := range globalHub.roomHistory(sendMessage.GetRoomId()) {
					select {
					case c.send <- past:
					default:
					}
				}
			}

			newMsg := &chatPb.NewMessageEvent{
				MessageId: fmt.Sprintf("%d", time.Now().UnixNano()),
				RoomId:    sendMessage.GetRoomId(),
				SenderId:  c.userId,
				Text:      sendMessage.GetText(),
				Timestamp: time.Now().Format(time.RFC3339),
			}
			response := &chatPb.ChatResponse{
				Event: &chatPb.ChatResponse_NewMessage{
					NewMessage: newMsg,
				},
			}
			globalHub.appendHistory(sendMessage.GetRoomId(), response)
			// Broadcast to room
			globalHub.broadcast(sendMessage.GetRoomId(), response)

		case *chatPb.ChatRequest_TypingStatus:
			tr := p.TypingStatus
			if tr == nil {
				continue
			}
			if tr.GetRoomId() == "" || tr.GetRoomId() == lobbyRoomName {
				continue
			}
			if globalHub.addClientToRoom(tr.GetRoomId(), c) {
				globalHub.broadcast(tr.GetRoomId(), systemNotification(
					tr.GetRoomId(),
					"joined",
					fmt.Sprintf("%s joined room %s", c.userId, tr.GetRoomId()),
				))
				for _, past := range globalHub.roomHistory(tr.GetRoomId()) {
					select {
					case c.send <- past:
					default:
					}
				}
			}
			typing := &chatPb.UserTypingEvent{
				RoomId:   tr.GetRoomId(),
				UserId:   c.userId,
				IsTyping: tr.GetIsTyping(),
			}
			response := &chatPb.ChatResponse{
				Event: &chatPb.ChatResponse_UserTyping{
					UserTyping: typing,
				},
			}
			globalHub.broadcast(tr.GetRoomId(), response)
		default: // Unknown payload; ignore
		}
	}

	// Client disconnected: cleanup
	for _, room := range globalHub.removeClient(c) {
		globalHub.broadcast(room, systemNotification(
			room,
			"left",
			fmt.Sprintf("%s left room %s", c.userId, room),
		))
	}
	close(c.send)
	wg.Wait()
	if sendErr != nil {
		return sendErr
	}
	return nil
}
