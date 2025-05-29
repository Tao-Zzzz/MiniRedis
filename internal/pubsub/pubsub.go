package pubsub

import (
	"sync"
)

// PubSub 管理发布/订阅
type PubSub struct {
	mu         sync.RWMutex
	channels   map[string]map[string]bool // 频道到客户端的映射
	messages   map[string][]string        // 客户端的消息队列
}

// NewPubSub 创建新的PubSub实例
func NewPubSub() *PubSub {
	return &PubSub{
		channels: make(map[string]map[string]bool),
		messages: make(map[string][]string),
	}
}

// Subscribe 订阅频道
func (ps *PubSub) Subscribe(clientID, channel string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if _, exists := ps.channels[channel]; !exists {
		ps.channels[channel] = make(map[string]bool)
	}
	ps.channels[channel][clientID] = true
}

// Unsubscribe 取消订阅频道
func (ps *PubSub) Unsubscribe(clientID, channel string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if clients, exists := ps.channels[channel]; exists {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(ps.channels, channel)
		}
	}
}

// UnsubscribeAll 取消客户端所有订阅
func (ps *PubSub) UnsubscribeAll(clientID string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for channel, clients := range ps.channels {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(ps.channels, channel)
		}
	}
	delete(ps.messages, clientID)
}

// Publish 发布消息到频道
func (ps *PubSub) Publish(channel, message string) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	clients, exists := ps.channels[channel]
	if !exists {
		return 0
	}

	count := 0
	for clientID := range clients {
		ps.messages[clientID] = append(ps.messages[clientID], message)
		count++
	}
	return count
}

// GetMessages 获取客户端的消息
func (ps *PubSub) GetMessages(clientID string) []string {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	messages := ps.messages[clientID]
	ps.messages[clientID] = nil
	return messages
}