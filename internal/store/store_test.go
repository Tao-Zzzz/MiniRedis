package store

import (
	"os"
	"testing"
	"time"
	"fmt"
)

type mockPubSub struct{}

func (m *mockPubSub) Publish(channel, message string) int { return 0 }

func TestStore(t *testing.T) {
	s := NewStore()
	s.SetPubSub(&mockPubSub{})

	// 测试SET和GET
	clientID := "test-client"
	s.Set(clientID, "key1", "value1", 0)
	if value, exists := s.Get(clientID, "key1"); !exists || value != "value1" {
		t.Errorf("Expected key1=value1, got %v", value)
	}

	fmt.Println("成功设置和获取 key1") 
	// 测试EXPIRE
	s.Set(clientID, "key2", "value2", 1)
	time.Sleep(2 * time.Second)
	if _, exists := s.Get(clientID, "key2"); exists {
		t.Error("Expected key2 to expire")
	}

	fmt.Println("成功设置和过期 key2") 

	// 测试LPUSH和LPOP
	s.LPush(clientID, "mylist", "item1", "item2")
	if length := s.LLen(clientID, "mylist"); length != 2 {
		t.Errorf("Expected list length 2, got %d", length)
	}
	if value, exists := s.LPop(clientID, "mylist"); !exists || value != "item2" {
		t.Errorf("Expected LPOP item2, got %v", value)
	}

	fmt.Println("成功LPUSH和LPOP mylist")

	// 测试事务
	s.BeginTransaction(clientID)
	
	fmt.Println("开始事务") 
	
	s.Set(clientID, "key3", "value3", 0)
	
	fmt.Println("设置事务中的 key3")

	s.LPush(clientID, "mylist2", "item3")
	
	fmt.Println("设置事务中的 mylist2")

	results := s.ExecuteTransaction(clientID)

	fmt.Println("执行完事务")

	if len(results) != 2 || results[0] != "+OK\r\n" || results[1] != ":1\r\n" {
		t.Errorf("Expected transaction results [OK, 1], got %v", results)
	}

	fmt.Println("成功执行事务") 
	// 测试LRU
	s.maxMemory = 10 // 极小内存限制
	s.Set(clientID, "key4", "value4", 0)
	s.Set(clientID, "key5", "value5", 0)
	time.Sleep(time.Millisecond)
	s.Get(clientID, "key5") // 更新访问时间
	s.checkMemory()
	if _, exists := s.Get(clientID, "key4"); exists {
		t.Error("Expected key4 to be evicted")
	}
	if _, exists := s.Get(clientID, "key5"); !exists {
		t.Error("Expected key5 to remain")
	}

	fmt.Println("成功LRU淘汰 key4") 

	// 清理AOF文件
	s.aof.Close()
	os.Remove("miniredis.aof")
	os.Remove("miniredis.rdb")
	
	fmt.Println("成功清理AOF和RDB文件")
}