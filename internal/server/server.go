package server

import (
	"bufio"
	"context"
	"log"
	"MiniRedis/internal/protocol"
	"MiniRedis/internal/pubsub"
	"MiniRedis/internal/store"
	"net"
	"fmt"
	"sync"
)

// Server 代表MiniRedis服务器
type Server struct {
    listener    net.Listener
    store       *store.Store
    pubsub      *pubsub.PubSub
    isSlave     bool
    masterAddr  string
    replication *Replication
    wg          sync.WaitGroup
    cmdCh       chan clientCommand
    subscribers sync.Map // 跟踪订阅状态的客户端
}

// clientCommand 代表客户端命令
type clientCommand struct {
	clientID string
	cmd      *protocol.Command
	conn     net.Conn
}

// NewServer 创建服务器实例
func NewServer(addr string, isSlave bool, masterAddr string) (*Server, error) {
	// 创建TCP监听器
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &Server{
		listener:   listener,
		store:      store.NewStore(),
		pubsub:     pubsub.NewPubSub(),
		isSlave:    isSlave,
		masterAddr: masterAddr,
		cmdCh:      make(chan clientCommand, 100),  // 带缓冲的命令通道
	}
	// 关联存储和发布订阅
	srv.store.SetPubSub(srv.pubsub)
	
	// 如果是从服务器，初始化复制模块	
	if isSlave && masterAddr != "" {
		srv.replication = NewReplication(masterAddr, srv.store, srv.cmdCh)
	}
	return srv, nil
}

// Start 启动服务器
func (s *Server) Start(ctx context.Context) error {
	// 启动事件循环
	go s.eventLoop(ctx)

	// 如果是从服务器，连接主服务器
	if s.isSlave && s.masterAddr != "" {
		go s.replication.Start(ctx)
	}

	// 主循环：接受客户端连接
	for {
		select {
		// 如果上下文被取消，关闭监听器并返回错误
		case <-ctx.Done():
			s.listener.Close()
			return ctx.Err()
		default:
			// 接受新的连接
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return err
				}
			}
			// 将连接计数	
			s.wg.Add(1)
			go s.handleConnection(ctx, conn)
		}
	}
}

// Wait 等待所有连接关闭
func (s *Server) Wait() {
	s.wg.Wait()
	close(s.cmdCh)
}

// eventLoop 处理命令和Pub/Sub消息
func (s *Server) eventLoop(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            return
        case cmd := <-s.cmdCh:
			// 处理命令并获取响应
            response := s.handleCommand(cmd)
			// 发送命令响应
            if response != "" {
                _, err := cmd.conn.Write([]byte(response))
                if err != nil {
                    log.Printf("Error writing to client %s: %v", cmd.clientID, err)
                    s.pubsub.UnsubscribeAll(cmd.clientID)
                    cmd.conn.Close()
                }
            }
            // 发送Pub/Sub消息（如果有）
            if messages := s.pubsub.GetMessages(cmd.clientID); len(messages) > 0 {
                for _, msg := range messages {
                    _, err := cmd.conn.Write([]byte(msg))
                    if err != nil {
                        log.Printf("Error writing to client %s: %v", cmd.clientID, err)
                        s.pubsub.UnsubscribeAll(cmd.clientID)
                        cmd.conn.Close()
                        break
                    }
                }
            }
        }
    }
}

// handleConnection 处理客户端连接
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
    // 如果事件循环结束,那么释放资源
	defer s.wg.Done()
    defer conn.Close()

    reader := bufio.NewReader(conn)
	// 生成客户端ID，使用远程地址作为唯一标识
    clientID := conn.RemoteAddr().String() 

    for {
        select {
        case <-ctx.Done():
            s.pubsub.UnsubscribeAll(clientID)
            s.subscribers.Delete(clientID)
            return
        default:
            //检查是否处于订阅状态
            if _, isSubscriber := s.subscribers.Load(clientID); isSubscriber {
                // 如果是订阅者，尝试推送消息
                if messages := s.pubsub.GetMessages(clientID); len(messages) > 0 {
                    for _, msg := range messages {
                        _, err := conn.Write([]byte(msg))
                        if err != nil {
                            log.Printf("Error writing to client %s: %v", clientID, err)
                            s.pubsub.UnsubscribeAll(clientID)
                            s.subscribers.Delete(clientID)
                            conn.Close()
                            return
                        }
                    }
                }
                continue // 继续循环，等待消息
            }
            // 正常读取命令
            cmd, err := protocol.ParseRESP(reader)

			fmt.Printf("Received command from %s: %s\n", clientID, cmd.Name)
            
			if err != nil {
                if err.Error() == "EOF" {
                    log.Printf("Client %s disconnected", clientID)
                    s.pubsub.UnsubscribeAll(clientID)
                    s.subscribers.Delete(clientID)
                    return
                }
                protocol.WriteError(conn, "ERR invalid command")
                continue
            }
            // 如果是 SUBSCRIBE 命令，标记为订阅者
            if cmd.Name == "SUBSCRIBE" {
                s.subscribers.Store(clientID, true)
            }
            // 提交命令到事件循环
            s.cmdCh <- clientCommand{clientID: clientID, cmd: cmd, conn: conn}
        }
    }
}

// handleCommand 处理命令
func (s *Server) handleCommand(cmd clientCommand) string {
    clientID := cmd.clientID

    // 从服务器只允许读命令
    if s.isSlave && !isReadCommand(cmd.cmd.Name) {
        return "-ERR write commands not allowed on slave\r\n"
    }

    switch cmd.cmd.Name {
    case "SET":
        if len(cmd.cmd.Args) < 2 {
            return "-ERR wrong number of arguments for 'SET' command\r\n"
        }
		var expire int = 0
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
		// 存数据
        s.store.Set(clientID, cmd.cmd.Args[0], cmd.cmd.Args[1], expire)
        // 主服务器复制命令到从服务器
		if !s.isSlave && s.replication != nil {
            s.replication.SendCommand(cmd.cmd)
        }

		// 发送键空间通知
        s.pubsub.Publish("keyspace", protocol.SerializeArray([]string{
            protocol.SerializeBulkString("message"),
            protocol.SerializeBulkString("keyspace"),
            protocol.SerializeBulkString("SET:" + cmd.cmd.Args[0]),
        }))
        return "+OK\r\n"
    case "GET":
        if len(cmd.cmd.Args) != 1 {
            return "-ERR wrong number of arguments for 'GET' command\r\n"
        }
        if value, exists := s.store.Get(clientID, cmd.cmd.Args[0]); exists {
            return protocol.SerializeBulkString(value)
        }
        return "$-1\r\n"
    case "DEL":
        if len(cmd.cmd.Args) < 1 {
            return "-ERR wrong number of arguments for 'DEL' command\r\n"
        }
        count := s.store.Delete(clientID, cmd.cmd.Args...)
        if !s.isSlave && s.replication != nil {
            s.replication.SendCommand(cmd.cmd)
        }
        for _, key := range cmd.cmd.Args {
            s.pubsub.Publish("keyspace", protocol.SerializeArray([]string{
                protocol.SerializeBulkString("message"),
                protocol.SerializeBulkString("keyspace"),
                protocol.SerializeBulkString("DEL:" + key),
            }))
        }
        return protocol.SerializeInteger(count)
    case "EXISTS":
        if len(cmd.cmd.Args) < 1 {
            return "-ERR wrong number of arguments for 'EXISTS' command\r\n"
        }
        count := s.store.Exists(clientID, cmd.cmd.Args...)
        return protocol.SerializeInteger(count)
	case "EXPIRE":
		if len(cmd.cmd.Args) != 2 {
			return "-ERR wrong number of arguments for 'EXPIRE' command\r\n"
		}
		seconds, err := protocol.ParseInt(cmd.cmd.Args[1])
		if err != nil {
			return "-ERR invalid expire time\r\n"
		}
		if s.store.Expire(clientID, cmd.cmd.Args[0], seconds) {
			if !s.isSlave && s.replication != nil {
				s.replication.SendCommand(cmd.cmd)
			}
			// 发布键空间通知
			// s.pubsub.Publish("keyspace", protocol.SerializeArray([]string{
			// 	protocol.SerializeBulkString("message"),
			// 	protocol.SerializeBulkString("keyspace"),
			// 	protocol.SerializeBulkString("EXPIRE:" + cmd.cmd.Args[0]),
			// }))
			return ":1\r\n"
		}
		return ":0\r\n"
    case "LPUSH":
        if len(cmd.cmd.Args) < 2 {
            return "-ERR wrong number of arguments for 'LPUSH' command\r\n"
        }
        length := s.store.LPush(clientID, cmd.cmd.Args[0], cmd.cmd.Args[1:]...)
        if !s.isSlave && s.replication != nil {
            s.replication.SendCommand(cmd.cmd)
        }
        return protocol.SerializeInteger(length)
    case "RPUSH":
        if len(cmd.cmd.Args) < 2 {
            return "-ERR wrong number of arguments for 'RPUSH' command\r\n"
        }
        length := s.store.RPush(clientID, cmd.cmd.Args[0], cmd.cmd.Args[1:]...)
        if !s.isSlave && s.replication != nil {
            s.replication.SendCommand(cmd.cmd)
        }
        return protocol.SerializeInteger(length)
    case "LPOP":
        if len(cmd.cmd.Args) != 1 {
            return "-ERR wrong number of arguments for 'LPOP' command\r\n"
        }
        if value, exists := s.store.LPop(clientID, cmd.cmd.Args[0]); exists {
            if !s.isSlave && s.replication != nil {
                s.replication.SendCommand(cmd.cmd)
            }
            return protocol.SerializeBulkString(value)
        }
        return "$-1\r\n"
    case "RPOP":
        if len(cmd.cmd.Args) != 1 {
            return "-ERR wrong number of arguments for 'RPOP' command\r\n"
        }
        if value, exists := s.store.RPop(clientID, cmd.cmd.Args[0]); exists {
            if !s.isSlave && s.replication != nil {
                s.replication.SendCommand(cmd.cmd)
            }
            return protocol.SerializeBulkString(value)
        }
        return "$-1\r\n"
    case "LLEN":
        if len(cmd.cmd.Args) != 1 {
            return "-ERR wrong number of arguments for 'LLEN' command\r\n"
        }
        length := s.store.LLen(clientID, cmd.cmd.Args[0])
        return protocol.SerializeInteger(length)
    case "MULTI":
        if len(cmd.cmd.Args) != 0 {
            return "-ERR wrong number of arguments for 'MULTI' command\r\n"
        }
		// 开始事务, 以客户端ID为标识
        s.store.BeginTransaction(clientID)
        return "+OK\r\n"
    case "EXEC":
        if len(cmd.cmd.Args) != 0 {
            return "-ERR wrong number of arguments for 'EXEC' command\r\n"
        }
        results := s.store.ExecuteTransaction(clientID)

		// 复制事务所中的所有命令
        if !s.isSlave && s.replication != nil {
            for _, cmd := range s.store.GetTransactionCommands(clientID) {
                s.replication.SendCommand(cmd)
            }
        }
        return protocol.SerializeArray(results)
    case "DISCARD":
        if len(cmd.cmd.Args) != 0 {
            return "-ERR wrong number of arguments for 'DISCARD' command\r\n"
        }
        s.store.DiscardTransaction(clientID)
        return "+OK\r\n"
    case "PUBLISH":
        if len(cmd.cmd.Args) != 2 {
            return "-ERR wrong number of arguments for 'PUBLISH' command\r\n"
        }
        count := s.pubsub.Publish(cmd.cmd.Args[0], protocol.SerializeArray([]string{
            protocol.SerializeBulkString("message"),
            protocol.SerializeBulkString(cmd.cmd.Args[0]),
            protocol.SerializeBulkString(cmd.cmd.Args[1]),
        }))
        return protocol.SerializeInteger(count)
    case "SUBSCRIBE":
        if len(cmd.cmd.Args) < 1 {
            return "-ERR wrong number of arguments for 'SUBSCRIBE' command\r\n"
        }
        for _, channel := range cmd.cmd.Args {
            s.pubsub.Subscribe(clientID, channel)
            // 直接写入每个频道的订阅确认
            response := protocol.SerializeArray([]string{
                protocol.SerializeBulkString("subscribe"),
                protocol.SerializeBulkString(channel),
                protocol.SerializeInteger(s.pubsub.CountSubscriptions(clientID)),
            })
            _, err := cmd.conn.Write([]byte(response))
            if err != nil {
                log.Printf("Error writing to client %s: %v", clientID, err)
                s.pubsub.UnsubscribeAll(clientID)
                cmd.conn.Close()
                return ""
            }
        }
        return "" // 不返回响应，因为已直接写入
    case "UNSUBSCRIBE":
        if len(cmd.cmd.Args) < 1 {
            return "-ERR wrong number of arguments for 'UNSUBSCRIBE' command\r\n"
        }
        for _, channel := range cmd.cmd.Args {
            s.pubsub.Unsubscribe(clientID, channel)
            response := protocol.SerializeArray([]string{
                protocol.SerializeBulkString("unsubscribe"),
                protocol.SerializeBulkString(channel),
                protocol.SerializeInteger(s.pubsub.CountSubscriptions(clientID)),
            })
            _, err := cmd.conn.Write([]byte(response))
            if err != nil {
                log.Printf("Error writing to client %s: %v", clientID, err)
                s.pubsub.UnsubscribeAll(clientID)
                s.subscribers.Delete(clientID)
                cmd.conn.Close()
                return ""
            }
        }
        // 如果客户端不再订阅任何频道，移除订阅者状态
        if s.pubsub.CountSubscriptions(clientID) == 0 {
            s.subscribers.Delete(clientID)
        }
        return ""
    default:
        return "-ERR unknown command '" + cmd.cmd.Name + "'\r\n"
    }
}

// isReadCommand 判断命令是否为读命令
func isReadCommand(name string) bool {
	return name == "GET" || name == "EXISTS" || name == "LLEN" || name == "SUBSCRIBE" || name == "UNSUBSCRIBE"
}