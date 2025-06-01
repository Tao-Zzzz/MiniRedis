// 声明包名为 server
package server

// 导入依赖包
import (
	"bufio"       // 带缓冲的 I/O 操作
	"context"     // 上下文管理，用于控制并发操作
	"fmt"         // 格式化 I/O
	"log"         // 日志记录
	"MiniRedis/internal/protocol" // 协议处理模块
	"MiniRedis/internal/pubsub"   // 发布订阅功能
	"MiniRedis/internal/store"    // 数据存储
	"net"         // 网络操作
	"sync"        // 同步原语，用于并发控制
)

// Server 代表 MiniRedis 服务器
type Server struct {
	listener      net.Listener   // 网络监听器
	store         *store.Store   // 数据存储实例
	pubsub        *pubsub.PubSub // 发布订阅系统
	isSlave       bool           // 是否是从节点
	masterAddr    string         // 主节点地址（如果是从节点）
	replication   *Replication    // 复制管理器
	wg            sync.WaitGroup // 等待组，用于优雅关闭
	cmdCh         chan clientCommand // 命令处理通道
	subscribers   sync.Map       // 并发安全的订阅者映射
	slaves        sync.Map       // 并发安全的从节点映射
}

// clientCommand 代表客户端命令封装
type clientCommand struct {
	clientID string          // 客户端唯一标识（通常使用远程地址）
	cmd      *protocol.Command // 解析后的命令对象
	conn     net.Conn        // 客户端连接
}

// NewServer 创建服务器实例
func NewServer(addr string, isSlave bool, masterAddr string) (*Server, error) {
	// 创建 TCP 监听器
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	
	// 初始化服务器结构体
	srv := &Server{
		listener:    listener,
		store:       store.NewStore(),    // 创建新的数据存储
		pubsub:      pubsub.NewPubSub(),  // 创建新的发布订阅系统
		isSlave:     isSlave,             // 设置从节点标识
		masterAddr:  masterAddr,           // 设置主节点地址
		cmdCh:       make(chan clientCommand, 100), // 缓冲通道用于命令处理
	}
	
	// 为存储设置关联的发布订阅系统
	srv.store.SetPubSub(srv.pubsub)
	
	// 配置复制功能
	if isSlave && masterAddr != "" {
		// 作为从节点时，创建完整的复制管理器
		srv.replication = NewReplication(masterAddr, srv.store, srv.cmdCh)
	} else {
		// 主节点或独立节点时的简化复制管理器
		srv.replication = &Replication{
			store: srv.store,
			cmdCh: srv.cmdCh,
		}
	}
	
	return srv, nil
}

// Start 启动服务器
func (s *Server) Start(ctx context.Context) error {
	// 启动事件循环协程处理命令和消息
	go s.eventLoop(ctx)

	// 如果是从节点且有主节点地址，启动复制协程
	if s.isSlave && s.masterAddr != "" {
		go s.replication.Start(ctx)
	}

	// 主循环：接受新连接
	for {
		select {
		case <-ctx.Done(): // 上下文取消时停止
			s.listener.Close() // 关闭监听器
			return ctx.Err()   // 返回取消原因
		default:
			// 接受新的客户端连接
			conn, err := s.listener.Accept()
			if err != nil {
				// 检查是否是正常关闭
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return err
				}
			}
			
			// 为每个连接增加等待组计数
			s.wg.Add(1)
			
			// 启动新的协程处理客户端连接
			go s.handleConnection(ctx, conn)
		}
	}
}

// Wait 等待所有连接关闭
func (s *Server) Wait() {
	// 阻塞直到所有连接处理完成
	s.wg.Wait()
	
	// 安全关闭命令通道
	close(s.cmdCh)
}

// eventLoop 处理命令和Pub/Sub消息的事件循环
func (s *Server) eventLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done(): // 上下文取消时退出
			return
		case cmd := <-s.cmdCh: // 从命令通道接收命令
			// 处理命令并获取响应
			response := s.handleCommand(cmd)
			
			// 非空响应发送给客户端
			if response != "" && cmd.conn != nil {
				_, err := cmd.conn.Write([]byte(response))
				if err != nil {
					// 写失败时记录错误并清理
					log.Printf("Error writing to client %s: %v", cmd.clientID, err)
					s.pubsub.UnsubscribeAll(cmd.clientID) // 取消所有订阅
					cmd.conn.Close()                     // 关闭连接
				}
			}
			
			// 检查并发送所有待处理的订阅消息
			if messages := s.pubsub.GetMessages(cmd.clientID); len(messages) > 0 {
				for _, msg := range messages {
					_, err := cmd.conn.Write([]byte(msg))
					if err != nil {
						// 写失败时记录错误并清理
						log.Printf("Error writing to client %s: %v", cmd.clientID, err)
						s.pubsub.UnsubscribeAll(cmd.clientID) // 取消所有订阅
						cmd.conn.Close()                     // 关闭连接
						break
					}
				}
			}
		}
	}
}

// handleConnection 处理单个客户端连接
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	// 确保结束时减少等待组计数并关闭连接
	defer s.wg.Done()
	defer conn.Close()

	// 创建带缓冲的读取器
	reader := bufio.NewReader(conn)
	
	// 使用远程地址作为客户端ID
	clientID := conn.RemoteAddr().String()

	// 主循环：处理来自客户端的请求
	for {
		select {
		case <-ctx.Done(): // 上下文取消时清理
			s.pubsub.UnsubscribeAll(clientID) // 取消所有订阅
			s.subscribers.Delete(clientID)   // 从订阅者映射删除
			s.slaves.Delete(clientID)        // 从从节点映射删除
			return
		default:
			// 检查是否是订阅者
			if _, isSubscriber := s.subscribers.Load(clientID); isSubscriber {
				// 获取并发送所有待处理的订阅消息
				if messages := s.pubsub.GetMessages(clientID); len(messages) > 0 {
					for _, msg := range messages {
						_, err := conn.Write([]byte(msg))
						if err != nil {
							// 写失败时清理并结束连接
							log.Printf("Error writing to client %s: %v", clientID, err)
							s.pubsub.UnsubscribeAll(clientID)
							s.subscribers.Delete(clientID)
							conn.Close()
							return
						}
					}
				}
				continue
			}
			
			// 解析 RESP 协议格式的命令
			cmd, err := protocol.ParseRESP(reader)
			if err != nil {
				// 连接关闭处理
				if err.Error() == "EOF" {
					log.Printf("Client %s disconnected", clientID)
					s.pubsub.UnsubscribeAll(clientID)
					s.subscribers.Delete(clientID)
					s.slaves.Delete(clientID)
					return
				}
				// 其他错误返回错误响应
				protocol.WriteError(conn, "ERR invalid command")
				continue
			}
			
			// 日志记录收到的命令
			fmt.Printf("Received command from %s: %s\n", clientID, cmd.Name)
			
			// 特殊处理 SLAVEOF 命令
			if cmd.Name == "SLAVEOF" {
				// 保存从服务器连接
				s.slaves.Store(clientID, conn)
				
				// 注册从服务器到复制管理器
				s.replication.RegisterSlave(clientID, conn)
				continue
			}
			
			// 标记订阅者
			if cmd.Name == "SUBSCRIBE" {
				s.subscribers.Store(clientID, true)
			}
			
			// 发送命令到处理通道
			s.cmdCh <- clientCommand{clientID: clientID, cmd: cmd, conn: conn}
		}
	}
}

// handleCommand 处理具体命令并返回响应
func (s *Server) handleCommand(cmd clientCommand) string {
	clientID := cmd.clientID
	// 日志记录处理命令
	log.Printf("Handling command %s from client %s", cmd.cmd.Name, clientID)
	
	// 从节点写命令拦截（除了复制连接）
	if s.isSlave && !isReadCommand(cmd.cmd.Name) && clientID != "replication" {
		log.Printf("Write command %s blocked on slave from client %s", cmd.cmd.Name, clientID)
		return "-ERR write commands not allowed on slave\r\n"
	}

	// 命令分发处理器
	switch cmd.cmd.Name {
	case "SLAVEOF":
		// 从节点不允许使用 SLAVEOF
		if s.isSlave {
			return "-ERR SLAVEOF not allowed on slave\r\n"
		}
		// 参数校验
		if len(cmd.cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'SLAVEOF' command\r\n"
		}
		// 确认从服务器连接
		return "+OK\r\n"
	
	case "SET":
		// SET key value [EX seconds]
		if len(cmd.cmd.Args) < 2 {
			return "-ERR wrong number of arguments for 'SET' command\r\n"
		}
		var expire int = 0
		// 处理过期时间参数
		if len(cmd.cmd.Args) >= 3 {
			if cmd.cmd.Args[2] == "EX" && len(cmd.cmd.Args) == 4 {
				var err error
				expire, err = protocol.ParseInt(cmd.cmd.Args[3])
				if err != nil {
					return "-ERR invalid expire time\r\n"
				}
			} else {
				return "-ERR syntax error\r\n"
			}
		}
		// 存储键值
		s.store.Set(clientID, cmd.cmd.Args[0], cmd.cmd.Args[1], expire)
		// 主节点需要复制到从节点
		if !s.isSlave && s.replication != nil {
			s.replication.SendCommand(cmd.cmd)
		}
		// 发布键空间通知
		s.pubsub.Publish("keyspace", protocol.SerializeArray([]string{
			protocol.SerializeBulkString("message"),
			protocol.SerializeBulkString("keyspace"),
			protocol.SerializeBulkString("SET:" + cmd.cmd.Args[0]),
		}))
		return "+OK\r\n"
	
	case "GET":
		// GET key
		if len(cmd.cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'GET' command\r\n"
		}
		// 获取键值
		if value, exists := s.store.Get(clientID, cmd.cmd.Args[0]); exists {
			return protocol.SerializeBulkString(value)
		}
		return "$-1\r\n" // 空值响应
	
	case "DEL":
		// DEL key [key ...]
		if len(cmd.cmd.Args) < 1 {
			return "-ERR wrong number of arguments for 'DEL' command\r\n"
		}
		// 删除键值
		count := s.store.Delete(clientID, cmd.cmd.Args...)
		// 主节点需要复制到从节点
		if !s.isSlave && s.replication != nil {
			s.replication.SendCommand(cmd.cmd)
		}
		// 为每个键发布键空间通知
		for _, key := range cmd.cmd.Args {
			s.pubsub.Publish("keyspace", protocol.SerializeArray([]string{
				protocol.SerializeBulkString("message"),
				protocol.SerializeBulkString("keyspace"),
				protocol.SerializeBulkString("DEL:" + key),
			}))
		}
		return protocol.SerializeInteger(count) // 返回删除数量
	
	case "EXISTS":
		// EXISTS key [key ...]
		if len(cmd.cmd.Args) < 1 {
			return "-ERR wrong number of arguments for 'EXISTS' command\r\n"
		}
		// 检查键是否存在
		count := s.store.Exists(clientID, cmd.cmd.Args...)
		return protocol.SerializeInteger(count) // 返回存在数量
	
	case "EXPIRE":
		// EXPIRE key seconds
		if len(cmd.cmd.Args) != 2 {
			return "-ERR wrong number of arguments for 'EXPIRE' command\r\n"
		}
		// 解析过期时间
		seconds, err := protocol.ParseInt(cmd.cmd.Args[1])
		if err != nil {
			return "-ERR invalid expire time\r\n"
		}
		// 设置过期时间
		if s.store.Expire(clientID, cmd.cmd.Args[0], seconds) {
			// 主节点需要复制到从节点
			if !s.isSlave && s.replication != nil {
				s.replication.SendCommand(cmd.cmd)
			}
			return ":1\r\n" // 设置成功
		}
		return ":0\r\n" // 设置失败
	
	case "LPUSH":
		// LPUSH key value [value ...]
		if len(cmd.cmd.Args) < 2 {
			return "-ERR wrong number of arguments for 'LPUSH' command\r\n"
		}
		// 左推入列表
		length := s.store.LPush(clientID, cmd.cmd.Args[0], cmd.cmd.Args[1:]...)
		// 主节点需要复制到从节点
		if !s.isSlave && s.replication != nil {
			s.replication.SendCommand(cmd.cmd)
		}
		return protocol.SerializeInteger(length) // 返回列表长度
	
	case "RPUSH":
		// RPUSH key value [value ...]
		if len(cmd.cmd.Args) < 2 {
			return "-ERR wrong number of arguments for 'RPUSH' command\r\n"
		}
		// 右推入列表
		length := s.store.RPush(clientID, cmd.cmd.Args[0], cmd.cmd.Args[1:]...)
		// 主节点需要复制到从节点
		if !s.isSlave && s.replication != nil {
			s.replication.SendCommand(cmd.cmd)
		}
		return protocol.SerializeInteger(length) // 返回列表长度
	
	case "LPOP":
		// LPOP key
		if len(cmd.cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'LPOP' command\r\n"
		}
		// 左弹出元素
		if value, exists := s.store.LPop(clientID, cmd.cmd.Args[0]); exists {
			// 主节点需要复制到从节点
			if !s.isSlave && s.replication != nil {
				s.replication.SendCommand(cmd.cmd)
			}
			return protocol.SerializeBulkString(value) // 返回元素值
		}
		return "$-1\r\n" // 空列表响应
	
	case "RPOP":
		// RPOP key
		if len(cmd.cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'RPOP' command\r\n"
		}
		// 右弹出元素
		if value, exists := s.store.RPop(clientID, cmd.cmd.Args[0]); exists {
			// 主节点需要复制到从节点
			if !s.isSlave && s.replication != nil {
				s.replication.SendCommand(cmd.cmd)
			}
			return protocol.SerializeBulkString(value) // 返回元素值
		}
		return "$-1\r\n" // 空列表响应
	
	case "LLEN":
		// LLEN key
		if len(cmd.cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'LLEN' command\r\n"
		}
		// 获取列表长度
		length := s.store.LLen(clientID, cmd.cmd.Args[0])
		return protocol.SerializeInteger(length) // 返回长度值
	
	case "MULTI":
		// MULTI
		if len(cmd.cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'MULTI' command\r\n"
		}
		// 开始事务
		s.store.BeginTransaction(clientID)
		return "+OK\r\n"
	
	case "EXEC":
		// EXEC
		if len(cmd.cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'EXEC' command\r\n"
		}
		// 执行事务
		results := s.store.ExecuteTransaction(clientID)
		// 主节点需要将事务命令复制到从节点
		if !s.isSlave && s.replication != nil {
			for _, cmd := range s.store.GetTransactionCommands(clientID) {
				s.replication.SendCommand(cmd)
			}
		}
		return protocol.SerializeArray(results) // 返回事务结果数组
	
	case "DISCARD":
		// DISCARD
		if len(cmd.cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'DISCARD' command\r\n"
		}
		// 放弃事务
		s.store.DiscardTransaction(clientID)
		return "+OK\r\n"
	
	case "PUBLISH":
		// PUBLISH channel message
		if len(cmd.cmd.Args) != 2 {
			return "-ERR wrong number of arguments for 'PUBLISH' command\r\n"
		}
		// 发布消息并获取订阅者数量
		count := s.pubsub.Publish(cmd.cmd.Args[0], protocol.SerializeArray([]string{
			protocol.SerializeBulkString("message"),
			protocol.SerializeBulkString(cmd.cmd.Args[0]),
			protocol.SerializeBulkString(cmd.cmd.Args[1]),
		}))
		return protocol.SerializeInteger(count) // 返回接收消息的客户端数
	
	case "SUBSCRIBE":
		// SUBSCRIBE channel [channel ...]
		if len(cmd.cmd.Args) < 1 {
			return "-ERR wrong number of arguments for 'SUBSCRIBE' command\r\n"
		}
		// 处理每个订阅的频道
		for _, channel := range cmd.cmd.Args {
			// 订阅频道
			s.pubsub.Subscribe(clientID, channel)
			
			// 发送订阅确认消息
			response := protocol.SerializeArray([]string{
				protocol.SerializeBulkString("subscribe"),
				protocol.SerializeBulkString(channel),
				protocol.SerializeInteger(s.pubsub.CountSubscriptions(clientID)),
			})
			
			// 直接发送到连接
			_, err := cmd.conn.Write([]byte(response))
			if err != nil {
				// 发送失败时清理
				log.Printf("Error writing to client %s: %v", clientID, err)
				s.pubsub.UnsubscribeAll(clientID)
				cmd.conn.Close()
				return ""
			}
		}
		// 无后续响应（保持连接用于消息传输）
		return ""
	
	case "UNSUBSCRIBE":
		// UNSUBSCRIBE channel [channel ...]
		if len(cmd.cmd.Args) < 1 {
			return "-ERR wrong number of arguments for 'UNSUBSCRIBE' command\r\n"
		}
		// 处理每个退订的频道
		for _, channel := range cmd.cmd.Args {
			// 退订频道
			s.pubsub.Unsubscribe(clientID, channel)
			
			// 发送退订确认消息
			response := protocol.SerializeArray([]string{
				protocol.SerializeBulkString("unsubscribe"),
				protocol.SerializeBulkString(channel),
				protocol.SerializeInteger(s.pubsub.CountSubscriptions(clientID)),
			})
			
			_, err := cmd.conn.Write([]byte(response))
			if err != nil {
				// 发送失败时清理
				log.Printf("Error writing to client %s: %v", clientID, err)
				s.pubsub.UnsubscribeAll(clientID)
				s.subscribers.Delete(clientID)
				cmd.conn.Close()
				return ""
			}
		}
		// 如果没有订阅了，移出订阅者映射
		if s.pubsub.CountSubscriptions(clientID) == 0 {
			s.subscribers.Delete(clientID)
		}
		return ""
	
	default:
		// 未知命令错误
		return "-ERR unknown command '" + cmd.cmd.Name + "'\r\n"
	}
}

// isReadCommand 判断命令是否为读命令
func isReadCommand(name string) bool {
	// 允许在从节点执行的读命令列表
	return name == "GET" || 
		name == "EXISTS" || 
		name == "LLEN" || 
		name == "SUBSCRIBE" || 
		name == "UNSUBSCRIBE"
}