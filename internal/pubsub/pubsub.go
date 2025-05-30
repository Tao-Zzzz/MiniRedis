package pubsub

import (
	"sync"
)

// PubSub 管理发布/订阅
type PubSub struct {
	mu       sync.RWMutex         // 读写锁，保证并发安全
	channels map[string]map[string]bool // 频道到客户端的映射
	messages map[string]chan string    // 客户端到消息通道的映射
}

// NewPubSub 创建PubSub实例
func NewPubSub() *PubSub {
	return &PubSub{
		channels: make(map[string]map[string]bool),
		messages: make(map[string]chan string),
	}
}

// Subscribe 订阅频道
func (ps *PubSub) Subscribe(clientID, channel string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// 如果频道不存在，则创建一个新的频道
	if _, exists := ps.channels[channel]; !exists {
		ps.channels[channel] = make(map[string]bool)
	}

	// 添加客户端到频道
	ps.channels[channel][clientID] = true
	
	// 创建消息通道，如果不存在的话
	if _, exists := ps.messages[clientID]; !exists {
		ps.messages[clientID] = make(chan string, 100)
	}
}

// Unsubscribe 取消订阅
func (ps *PubSub) Unsubscribe(clientID, channel string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// 检查频道是否存在
	if clients, exists := ps.channels[channel]; exists {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(ps.channels, channel)
		}
	}
}

// UnsubscribeAll 取消所有订阅
func (ps *PubSub) UnsubscribeAll(clientID string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// 遍历所有频道，删除客户端订阅
	for channel, clients := range ps.channels {
		// 移除客户端
		delete(clients, clientID)
		
		// 如果频道没有其他客户端，删除频道
		if len(clients) == 0 {
			delete(ps.channels, channel)
		}
	}

	// 关闭客户端的消息通道
	if ch, exists := ps.messages[clientID]; exists {
		// 先关闭再删除
		close(ch)
		delete(ps.messages, clientID)
	}
}

// Publish 发布消息
func (ps *PubSub) Publish(channel, message string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	// 获取频道的订阅者
	clients, exists := ps.channels[channel]
	if !exists {
		return 0
	}

	count := 0
	for clientID := range clients {
		if ch, exists := ps.messages[clientID]; exists {
			select {
			case ch <- message:
				count++
			default:
				// 通道满，跳过
			}
		}
	}
	return count
}


// CountSubscriptions 返回客户端订阅的频道数
func (ps *PubSub) CountSubscriptions(clientID string) int {
    ps.mu.RLock()
    defer ps.mu.RUnlock()
    
	// 遍历所有频道，统计客户端订阅的数量
    count := 0
    for _, clients := range ps.channels {
        if clients[clientID] {
            count++
        }
    }
    return count
}

// GetMessages 获取消息
func (ps *PubSub) GetMessages(clientID string) []string {
	// 获取客户端的消息通道
	ps.mu.RLock()
	ch, exists := ps.messages[clientID]
	ps.mu.RUnlock()

	if !exists {
		return nil
	}

	messages := []string{}
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return messages
			}
			messages = append(messages, msg)
		default:
			return messages
		}
	}
}