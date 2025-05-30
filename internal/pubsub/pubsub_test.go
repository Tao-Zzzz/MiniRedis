package pubsub

import (
	"testing"
)

func TestPubSub(t *testing.T) {
	ps := NewPubSub()

	// 测试订阅和发布
	clientID := "client1"
	ps.Subscribe(clientID, "channel1")
	count := ps.Publish("channel1", "*3\r\n$7\r\nmessage\r\n$8\r\nchannel1\r\n$4\r\ntest\r\n")
	if count != 1 {
		t.Errorf("Expected publish to 1 client, got %d", count)
	}

	messages := ps.GetMessages(clientID)
	if len(messages) != 1 || messages[0] != "*3\r\n$7\r\nmessage\r\n$8\r\nchannel1\r\n$4\r\ntest\r\n" {
		t.Errorf("Expected message 'test', got %v", messages)
	}

	// 测试取消订阅
	ps.Unsubscribe(clientID, "channel1")
	count = ps.Publish("channel1", "*3\r\n$7\r\nmessage\r\n$8\r\nchannel1\r\n$4\r\ntest2\r\n")
	if count != 0 {
		t.Errorf("Expected publish to 0 clients, got %d", count)
	}

	// 测试多客户端订阅
	ps.Subscribe("client2", "channel2")
	ps.Subscribe("client3", "channel2")
	count = ps.Publish("channel2", "*3\r\n$7\r\nmessage\r\n$8\r\nchannel2\r\n$4\r\ntest3\r\n")
	if count != 2 {
		t.Errorf("Expected publish to 2 clients, got %d", count)
	}

	// 测试UnsubscribeAll
	ps.UnsubscribeAll("client2")
	count = ps.Publish("channel2", "*3\r\n$7\r\nmessage\r\n$8\r\nchannel2\r\n$4\r\ntest4\r\n")
	if count != 1 {
		t.Errorf("Expected publish to 1 client, got %d", count)
	}
}