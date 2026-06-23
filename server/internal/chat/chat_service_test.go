package chat

import (
	"context"
	"testing"

	chatPb "todo-server/proto/gen"
)

func resetHub() {
	globalHub = &Hub{
		rooms: make(map[string]*roomState),
		users: make(map[string]map[string]struct{}),
	}
}

func TestRoomLifecycleRPCs(t *testing.T) {
	resetHub()
	srv := &Server{}
	ctx := context.Background()

	createResp, err := srv.CreateRoom(ctx, &chatPb.CreateRoomRequest{RoomId: "alpha", UserId: "u1"})
	if err != nil {
		t.Fatalf("CreateRoom error: %v", err)
	}
	if !createResp.GetCreated() {
		t.Fatalf("expected created=true")
	}

	joinResp, err := srv.JoinRoom(ctx, &chatPb.JoinRoomRequest{RoomId: "alpha", UserId: "u1"})
	if err != nil {
		t.Fatalf("JoinRoom error: %v", err)
	}
	if !joinResp.GetJoined() {
		t.Fatalf("expected joined=true")
	}

	roomsResp, err := srv.ListRooms(ctx, &chatPb.ListRoomsRequest{})
	if err != nil {
		t.Fatalf("ListRooms error: %v", err)
	}
	if len(roomsResp.GetRoomIds()) != 1 || roomsResp.GetRoomIds()[0] != "alpha" {
		t.Fatalf("unexpected room list: %v", roomsResp.GetRoomIds())
	}

	leaveResp, err := srv.LeaveRoom(ctx, &chatPb.LeaveRoomRequest{RoomId: "alpha", UserId: "u1"})
	if err != nil {
		t.Fatalf("LeaveRoom error: %v", err)
	}
	if !leaveResp.GetLeft() {
		t.Fatalf("expected left=true")
	}
}

func TestAutoJoinOnStreamActivity(t *testing.T) {
	resetHub()
	srv := &Server{}

	if _, err := srv.CreateRoom(context.Background(), &chatPb.CreateRoomRequest{RoomId: "room1"}); err != nil {
		t.Fatalf("CreateRoom error: %v", err)
	}
	if _, err := srv.JoinRoom(context.Background(), &chatPb.JoinRoomRequest{RoomId: "room1", UserId: "user-1"}); err != nil {
		t.Fatalf("JoinRoom error: %v", err)
	}

	rooms := globalHub.userRooms("user-1")
	if len(rooms) != 1 || rooms[0] != "room1" {
		t.Fatalf("unexpected joined rooms: %v", rooms)
	}
}
