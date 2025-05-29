package server

import (
	"bufio"                        // 缓冲读写库
	"log"                          // 日志库
	"MiniRedis/internal/protocol" // 导入我们自己写的 RESP 协议处理模块
	"MiniRedis/internal/store"    // 导入我们自己写的存储模块
	"net"                          // 提供网络通信能力（监听端口、处理连接等）
	"sync"                         // 提供同步原语，这里用的是 WaitGroup
	"context"
	"MiniRedis/internal/pubsub"
	"fmt"
)

// Server 代表MiniRedis服务器
type Server struct {
	listener net.Listener
	store    *store.Store
	pubsub   *pubsub.PubSub // 新增：Pub/Sub管理
	wg       sync.WaitGroup
}

// NewServer 创建一个新的服务器实例
func NewServer(addr string) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{
		listener: listener,
		store:    store.NewStore(),
		pubsub:   pubsub.NewPubSub(),
	}, nil
}

// Start 启动服务器，接受客户端连接
func (s *Server) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			s.listener.Close()
			return ctx.Err()
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return err
				}
			}
			s.wg.Add(1)
			go s.handleConnection(ctx, conn)
		}
	}
}

// Wait 等待所有连接处理完成
func (s *Server) Wait() {
	s.wg.Wait()
}

// handleConnection 处理单个客户端连接
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	reader := bufio.NewReader(conn)
	clientID := conn.RemoteAddr().String()
	for {
		select {
		case <-ctx.Done():
			s.pubsub.UnsubscribeAll(clientID)
			return
		default:
			// 读取并解析客户端发送的RESP命令
			cmd, err := protocol.ParseRESP(reader)
			if err != nil {
				if err.Error() == "EOF" {
					log.Printf("Client %s disconnected", conn.RemoteAddr())
					s.pubsub.UnsubscribeAll(clientID)
					return
				}
				protocol.WriteError(conn, "ERR invalid command")
				continue
			}

			// 处理命令
			response := s.handleCommand(clientID, cmd)
			// 如果客户端订阅了频道，可能有异步消息
			if messages := s.pubsub.GetMessages(clientID); len(messages) > 0 {
				for _, msg := range messages {
					_, err = conn.Write([]byte(msg))
					if err != nil {
						log.Printf("Error writing to client %s: %v", conn.RemoteAddr(), err)
						s.pubsub.UnsubscribeAll(clientID)
						return
					}
				}
			}
			// 发送命令响应
			if response != "" {
				_, err = conn.Write([]byte(response))
				if err != nil {
					log.Printf("Error writing to client %s: %v", conn.RemoteAddr(), err)
					s.pubsub.UnsubscribeAll(clientID)
					return
				}
			}
		}
	}
}

// handleCommand 处理解析后的命令
func (s *Server) handleCommand(clientID string, cmd *protocol.Command) string {
	switch cmd.Name {
	case "SET":
		if len(cmd.Args) < 2 {
			return "-ERR wrong number of arguments for 'SET' command\r\n"
		}
		var expire int
		if len(cmd.Args) >= 3 {
			if cmd.Args[2] == "EX" && len(cmd.Args) == 4 {
				var err error
				expire, err = protocol.ParseInt(cmd.Args[3])
				if err != nil {
					return "-ERR invalid expire time\r\n"
				}
			} else {
				return "-ERR syntax error\r\n"
			}
		}
		s.store.Set(clientID, cmd.Args[0], cmd.Args[1], expire)
		s.pubsub.Publish("keyspace", protocol.SerializeArray([]string{
			protocol.SerializeBulkString("message"),
			protocol.SerializeBulkString("keyspace"),
			protocol.SerializeBulkString("SET:" + cmd.Args[0]),
		}))
		return "+OK\r\n"
	case "GET":
		if len(cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'GET' command\r\n"
		}
		if value, exists := s.store.Get(clientID, cmd.Args[0]); exists {
			return protocol.SerializeBulkString(value)
		}
		return "$-1\r\n"
	case "DEL":
		if len(cmd.Args) < 1 {
			return "-ERR wrong number of arguments for 'DEL' command\r\n"
		}
		count := s.store.Delete(clientID, cmd.Args...)
		for _, key := range cmd.Args {
			s.pubsub.Publish("keyspace", protocol.SerializeArray([]string{
				protocol.SerializeBulkString("message"),
				protocol.SerializeBulkString("keyspace"),
				protocol.SerializeBulkString("DEL:" + key),
			}))
		}
		return protocol.SerializeInteger(count)
	case "EXISTS":
		if len(cmd.Args) < 1 {
			return "-ERR wrong number of arguments for 'EXISTS' command\r\n"
		}
		count := s.store.Exists(clientID, cmd.Args...)
		return protocol.SerializeInteger(count)
	case "EXPIRE":
		if len(cmd.Args) != 2 {
			return "-ERR wrong number of arguments for 'EXPIRE' command\r\n"
		}
		seconds, err := protocol.ParseInt(cmd.Args[1])
		if err != nil {
			return fmt.Sprintf("handling client %s: %v", clientID, err)
		}
		if s.store.Expire(clientID, cmd.Args[0], seconds) {
			return ":1\r\n"
		}
		return ":0\r\n"
	case "LPUSH":
		if len(cmd.Args) < 2 {
			return "-ERR wrong number of arguments for 'LPUSH' command\r\n"
		}
		length := s.store.LPush(clientID, cmd.Args[0], cmd.Args[1:]...)
		return protocol.SerializeInteger(length)
	case "RPUSH":
		if len(cmd.Args) < 2 {
			return "-ERR wrong number of arguments for 'RPUSH' command\r\n"
		}
		length := s.store.RPush(clientID, cmd.Args[0], cmd.Args[1:]...)
		return protocol.SerializeInteger(length)
	case "LPOP":
		if len(cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'LPOP' command\r\n"
		}
		if value, exists := s.store.LPop(clientID, cmd.Args[0]); exists {
			return protocol.SerializeBulkString(value)
		}
		return "$-1\r\n"
	case "RPOP":
		if len(cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'RPOP' command\r\n"
		}
		if value, exists := s.store.RPop(clientID, cmd.Args[0]); exists {
			return protocol.SerializeBulkString(value)
		}
		return "$-1\r\n"
	case "LLEN":
		if len(cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'LLEN' command\r\n"
		}
		length := s.store.LLen(clientID, cmd.Args[0])
		return protocol.SerializeInteger(length)
	case "MULTI":
		if len(cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'MULTI' command\r\n"
		}
		s.store.BeginTransaction(clientID)
		return "+OK\r\n"
	case "EXEC":
		if len(cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'EXEC' command\r\n"
		}
		results := s.store.ExecuteTransaction(clientID)
		return protocol.SerializeArray(results)
	case "DISCARD":
		if len(cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'DISCARD' command\r\n"
		}
		s.store.DiscardTransaction(clientID)
		return "+OK\r\n"
	case "PUBLISH":
		if len(cmd.Args) != 2 {
			return "-ERR wrong number of arguments for 'PUBLISH' command\r\n"
		}
		count := s.pubsub.Publish(cmd.Args[0], protocol.SerializeArray([]string{
			protocol.SerializeBulkString("message"),
			protocol.SerializeBulkString(cmd.Args[0]),
			protocol.SerializeBulkString(cmd.Args[1]),
		}))
		return protocol.SerializeInteger(count)
	case "SUBSCRIBE":
		if len(cmd.Args) < 1 {
			return "-ERR wrong number of arguments for 'SUBSCRIBE' command\r\n"
		}
		for _, channel := range cmd.Args {
			s.pubsub.Subscribe(clientID, channel)
		}
		return protocol.SerializeArray([]string{
			protocol.SerializeBulkString("subscribe"),
			protocol.SerializeBulkString(cmd.Args[0]),
			protocol.SerializeInteger(len(cmd.Args)),
		})
	case "UNSUBSCRIBE":
		if len(cmd.Args) < 1 {
			return "-ERR wrong number of arguments for 'UNSUBSCRIBE' command\r\n"
		}
		for _, channel := range cmd.Args {
			s.pubsub.Unsubscribe(clientID, channel)
		}
		return protocol.SerializeArray([]string{
			protocol.SerializeBulkString("unsubscribe"),
			protocol.SerializeBulkString(cmd.Args[0]),
			protocol.SerializeInteger(len(cmd.Args)),
		})
	default:
		return "-ERR unknown command '" + cmd.Name + "'\r\n"
	}
}