package session

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/lonng/nano/mock"
)

func TestStateAndRestoreCopyMaps(t *testing.T) {
	s := New(nil)
	input := map[string]interface{}{"value": 1}
	s.Restore(input)
	input["value"] = 2
	if s.Int("value") != 1 {
		t.Fatal("Restore retained caller map")
	}
	state := s.State()
	state["value"] = 3
	if s.Int("value") != 1 {
		t.Fatal("State exposed live map")
	}
	s.Restore(nil)
	s.Set("ok", true)
}
func TestConcurrentStateSnapshots(t *testing.T) {
	s := New(nil)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			s.Set("value", i)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			state := s.State()
			state["outside"] = i
		}
	}()
	wg.Wait()
	if s.HasKey("outside") {
		t.Fatal("snapshot writes escaped")
	}
}
func TestRequestViewSharesStateAndBoundsResponses(t *testing.T) {
	entity := mock.NewNetworkEntity()
	s := New(entity)
	ctx, cancel := context.WithCancel(context.Background())
	request := s.WithRequest(ctx, 42)
	request.Set("shared", true)
	request.Bind(7)
	if !s.HasKey("shared") || s.UID() != 7 || request.ID() != s.ID() {
		t.Fatal("request view detached storage")
	}
	if err := request.Response("response"); err != nil {
		t.Fatal(err)
	}
	if entity.FindResponseByMID(42) != "response" {
		t.Fatal("response ID was lost")
	}
	cancel()
	if err := request.Response("late"); !errors.Is(err, context.Canceled) {
		t.Fatalf("late response: %v", err)
	}
	if err := request.Push("room.tick", "tick"); err != nil {
		t.Fatalf("request cancellation broke connection push: %v", err)
	}
	if request.Connection().Context().Err() != nil {
		t.Fatal("connection retained request context")
	}
}
