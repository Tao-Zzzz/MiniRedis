package store

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// Store 代表键值存储
type Store struct {
	data         map[string]string            // 字符串键值对
	lists        map[string][]string          // 列表数据
	expire       map[string]time.Time         // 过期时间
	accessTime   map[string]time.Time         // 访问时间（LRU）
	transactions map[string][]Command         // 事务队列（按客户端ID）
	maxMemory    int                         // 最大内存（字节）
	mu           sync.RWMutex                 // 并发保护
	aof          *os.File                     // AOF文件
	pubsub       PubSub                      // 键空间通知接口
}

// PubSub 定义发布接口
type PubSub interface {
	Publish(channel, message string) int
}

// Command 代表事务中的命令
type Command struct {
	Name string
	Args []string
}

// NewStore 创建一个新的存储实例
func NewStore() *Store {
	f, err := os.OpenFile("miniredis.aof", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Warning: failed to open AOF file: %v\n", err)
	}
	s := &Store{
		data:         make(map[string]string),
		lists:        make(map[string][]string),
		expire:       make(map[string]time.Time),
		accessTime:   make(map[string]time.Time),
		transactions: make(map[string][]Command),
		maxMemory:    1024 * 1024 * 10, // 10MB
		aof:          f,
	}
	// 加载RDB快照
	data, lists, expire, err := LoadRDB("miniredis.rdb")
	if err == nil {
		s.data = data
		s.lists = lists
		s.expire = expire
		fmt.Println("Loaded RDB snapshot")
	} else if !os.IsNotExist(err) {
		fmt.Printf("Warning: failed to load RDB: %v\n", err)
	}
	// 启动定期快照和过期检查
	go s.startSnapshotting()
	go s.startExpirationCheck()
	return s
}

// SetPubSub 设置发布接口
func (s *Store) SetPubSub(ps PubSub) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pubsub = ps
}

// Set 设置键值对，可选设置过期时间（秒）
func (s *Store) Set(clientID, key, value string, expireSeconds int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx := s.getTransaction(clientID); tx != nil {
		tx = append(tx, Command{Name: "SET", Args: []string{key, value}})
		if expireSeconds > 0 {
			tx = append(tx, Command{Name: "EXPIRE", Args: []string{key, fmt.Sprintf("%d", expireSeconds)}})
		}
		s.transactions[clientID] = tx
		return
	}

	s.checkMemory()
	s.data[key] = value
	s.accessTime[key] = time.Now()
	if expireSeconds > 0 {
		s.expire[key] = time.Now().Add(time.Duration(expireSeconds) * time.Second)
	} else {
		delete(s.expire, key)
	}

	// 写入AOF日志
	if s.aof != nil {
		cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
		if expireSeconds > 0 {
			cmd += fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n", len(key), key, len(fmt.Sprintf("%d", expireSeconds)), expireSeconds)
		}
		s.aof.WriteString(cmd)
	}
}

// Get 获取键值
func (s *Store) Get(clientID, key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if expireTime, exists := s.expire[key]; exists && time.Now().After(expireTime) {
		s.mu.RUnlock()
		s.mu.Lock()
		delete(s.data, key)
		delete(s.expire, key)
		delete(s.accessTime, key)
		if s.pubsub != nil {
			s.pubsub.Publish("keyspace", fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$8\r\nkeyspace\r\n$%d\r\nexpired:%s\r\n", len("expired:"+key), "expired:"+key))
		}
		s.mu.Unlock()
		s.mu.RLock()
		return "", false
	}

	value, exists := s.data[key]
	if exists {
		s.mu.RUnlock()
		s.mu.Lock()
		s.accessTime[key] = time.Now()
		s.mu.Unlock()
		s.mu.RLock()
	}
	return value, exists
}

// Delete 删除一个或多个键
func (s *Store) Delete(clientID string, keys ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx := s.getTransaction(clientID); tx != nil {
		tx = append(tx, Command{Name: "DEL", Args: keys})
		s.transactions[clientID] = tx
		return 0
	}

	count := 0
	for _, key := range keys {
		if _, exists := s.data[key]; exists {
			delete(s.data, key)
			delete(s.expire, key)
			delete(s.accessTime, key)
			count++
			if s.aof != nil {
				cmd := fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key)
				s.aof.WriteString(cmd)
			}
		}
		if _, exists := s.lists[key]; exists {
			delete(s.lists, key)
			delete(s.accessTime, key)
			count++
			if s.aof != nil {
				cmd := fmt.Sprintf("*2\r\n$3\r\nDEL\r\n$%d\r\n%s\r\n", len(key), key)
				s.aof.WriteString(cmd)
			}
		}
	}
	return count
}

// Exists 检查一个或多个键是否存在
func (s *Store) Exists(clientID string, keys ...string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, key := range keys {
		if expireTime, exists := s.expire[key]; exists && time.Now().After(expireTime) {
			continue
		}
		if _, exists := s.data[key]; exists {
			s.mu.RUnlock()
			s.mu.Lock()
			s.accessTime[key] = time.Now()
			s.mu.Unlock()
			s.mu.RLock()
			count++
		}
		if _, exists := s.lists[key]; exists {
			s.mu.RUnlock()
			s.mu.Lock()
			s.accessTime[key] = time.Now()
			s.mu.Unlock()
			s.mu.RLock()
			count++
		}
	}
	return count
}

// Expire 设置键的过期时间
func (s *Store) Expire(clientID, key string, seconds int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx := s.getTransaction(clientID); tx != nil {
		tx = append(tx, Command{Name: "EXPIRE", Args: []string{key, fmt.Sprintf("%d", seconds)}})
		s.transactions[clientID] = tx
		return false
	}

	if _, exists := s.data[key]; !exists && !s.listExists(key) {
		return false
	}
	s.expire[key] = time.Now().Add(time.Duration(seconds) * time.Second)
	s.accessTime[key] = time.Now()
	if s.aof != nil {
		cmd := fmt.Sprintf("*3\r\n$6\r\nEXPIRE\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n", len(key), key, len(fmt.Sprintf("%d", seconds)), seconds)
		s.aof.WriteString(cmd)
	}
	return true
}

// LPush 向列表左侧插入元素
func (s *Store) LPush(clientID, key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx := s.getTransaction(clientID); tx != nil {
		tx = append(tx, Command{Name: "LPUSH", Args: append([]string{key}, values...)})
		s.transactions[clientID] = tx
		return 0
	}

	list, exists := s.lists[key]
	if !exists {
		list = []string{}
	}
	list = append(values, list...)
	s.lists[key] = list
	s.accessTime[key] = time.Now()
	if s.aof != nil {
		for _, value := range values {
			cmd := fmt.Sprintf("*3\r\n$5\r\nLPUSH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
			s.aof.WriteString(cmd)
		}
	}
	return len(list)
}

// RPush 向列表右侧插入元素
func (s *Store) RPush(clientID, key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx := s.getTransaction(clientID); tx != nil {
		tx = append(tx, Command{Name: "RPUSH", Args: append([]string{key}, values...)})
		s.transactions[clientID] = tx
		return 0
	}

	list, exists := s.lists[key]
	if !exists {
		list = []string{}
	}
	list = append(list, values...)
	s.lists[key] = list
	s.accessTime[key] = time.Now()
	if s.aof != nil {
		for _, value := range values {
			cmd := fmt.Sprintf("*3\r\n$5\r\nRPUSH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
			s.aof.WriteString(cmd)
		}
	}
	return len(list)
}

// LPop 从列表左侧弹出元素
func (s *Store) LPop(clientID, key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx := s.getTransaction(clientID); tx != nil {
		tx = append(tx, Command{Name: "LPOP", Args: []string{key}})
		s.transactions[clientID] = tx
		return "", false
	}

	list, exists := s.lists[key]
	if !exists || len(list) == 0 {
		return "", false
	}
	value := list[0]
	s.lists[key] = list[1:]
	if len(s.lists[key]) == 0 {
		delete(s.lists, key)
		delete(s.accessTime, key)
	}
	s.accessTime[key] = time.Now()
	if s.aof != nil {
		cmd := fmt.Sprintf("*2\r\n$4\r\nLPOP\r\n$%d\r\n%s\r\n", len(key), key)
		s.aof.WriteString(cmd)
	}
	return value, true
}

// RPop 从列表右侧弹出元素
func (s *Store) RPop(clientID, key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx := s.getTransaction(clientID); tx != nil {
		tx = append(tx, Command{Name: "RPOP", Args: []string{key}})
		s.transactions[clientID] = tx
		return "", false
	}

	list, exists := s.lists[key]
	if !exists || len(list) == 0 {
		return "", false
	}
	value := list[len(list)-1]
	s.lists[key] = list[:len(list)-1]
	if len(s.lists[key]) == 0 {
		delete(s.lists, key)
		delete(s.accessTime, key)
	}
	s.accessTime[key] = time.Now()
	if s.aof != nil {
		cmd := fmt.Sprintf("*2\r\n$4\r\nRPOP\r\n$%d\r\n%s\r\n", len(key), key)
		s.aof.WriteString(cmd)
	}
	return value, true
}

// LLen 获取列表长度
func (s *Store) LLen(clientID, key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list, exists := s.lists[key]
	if !exists {
		return 0
	}
	s.mu.RUnlock()
	s.mu.Lock()
	s.accessTime[key] = time.Now()
	s.mu.Unlock()
	s.mu.RLock()
	return len(list)
}

// listExists 检查列表是否存在
func (s *Store) listExists(key string) bool {
	_, exists := s.lists[key]
	return exists
}

// BeginTransaction 开始事务
func (s *Store) BeginTransaction(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transactions[clientID] = []Command{}
}

// ExecuteTransaction 执行事务
func (s *Store) ExecuteTransaction(clientID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, exists := s.transactions[clientID]
	if !exists {
		return []string{"-ERR no transaction to execute\r\n"}
	}
	delete(s.transactions, clientID)

	results := make([]string, 0, len(tx))
	for _, cmd := range tx {
		switch cmd.Name {
		case "SET":
			s.Set(clientID, cmd.Args[0], cmd.Args[1], 0)
			results = append(results, "+OK\r\n")
		case "EXPIRE":
			seconds, _ := time.ParseDuration(cmd.Args[1] + "s")
			s.Expire(clientID, cmd.Args[0], int(seconds.Seconds()))
			results = append(results, ":1\r\n")
		case "DEL":
			count := s.Delete(clientID, cmd.Args...)
			results = append(results, fmt.Sprintf(":%d\r\n", count))
		case "LPUSH":
			length := s.LPush(clientID, cmd.Args[0], cmd.Args[1:]...)
			results = append(results, fmt.Sprintf(":%d\r\n", length))
		case "RPUSH":
			length := s.RPush(clientID, cmd.Args[0], cmd.Args[1:]...)
			results = append(results, fmt.Sprintf(":%d\r\n", length))
		case "LPOP":
			if value, exists := s.LPop(clientID, cmd.Args[0]); exists {
				results = append(results, fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
			} else {
				results = append(results, "$-1\r\n")
			}
		case "RPOP":
			if value, exists := s.RPop(clientID, cmd.Args[0]); exists {
				results = append(results, fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
			} else {
				results = append(results, "$-1\r\n")
			}
		}
	}
	return results
}

// DiscardTransaction 丢弃事务
func (s *Store) DiscardTransaction(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.transactions, clientID)
}

// getTransaction 获取当前事务
func (s *Store) getTransaction(clientID string) []Command {
	return s.transactions[clientID]
}

// startSnapshotting 定期触发RDB快照
func (s *Store) startSnapshotting() {
	ticker := time.NewTicker(60 * time.Second)
	for range ticker.C {
		s.mu.RLock()
		Snapshot(s.data, s.lists, s.expire, "miniredis.rdb")
		s.mu.RUnlock()
	}
}

// startExpirationCheck 定期检查过期键
func (s *Store) startExpirationCheck() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		s.mu.Lock()
		for key, expireTime := range s.expire {
			if time.Now().After(expireTime) {
				delete(s.data, key)
				delete(s.lists, key)
				delete(s.expire, key)
				delete(s.accessTime, key)
				if s.pubsub != nil {
					s.pubsub.Publish("keyspace", fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$8\r\nkeyspace\r\n$%d\r\nexpired:%s\r\n", len("expired:"+key), "expired:"+key))
				}
			}
		}
		s.mu.Unlock()
	}
}

// checkMemory 检查内存使用并执行LRU淘汰
func (s *Store) checkMemory() {
	// 粗略估计内存使用量
	totalSize := 0
	for key, value := range s.data {
		totalSize += len(key) + len(value)
	}
	for key, list := range s.lists {
		totalSize += len(key)
		for _, value := range list {
			totalSize += len(value)
		}
	}

	if totalSize > s.maxMemory {
		// 按访问时间排序，删除最早的键
		type kv struct {
			key  string
			time time.Time
		}
		var keys []kv
		for key, t := range s.accessTime {
			keys = append(keys, kv{key, t})
		}
		for len(keys) > 0 && totalSize > s.maxMemory {
			// 找到最早访问的键
			minTime := time.Now()
			var minKey string
			for _, k := range keys {
				if k.time.Before(minTime) {
					minTime = k.time
					minKey = k.key
				}
			}
			// 删除键
			totalSize -= len(minKey)
			if value, exists := s.data[minKey]; exists {
				totalSize -= len(value)
				delete(s.data, minKey)
			}
			if list, exists := s.lists[minKey]; exists {
				for _, value := range list {
					totalSize -= len(value)
				}
				delete(s.lists, minKey)
			}
			delete(s.expire, minKey)
			delete(s.accessTime, minKey)
			// 从keys中移除
			for i, k := range keys {
				if k.key == minKey {
					keys = append(keys[:i], keys[i+1:]...)
					break
				}
			}
		}
	}
}