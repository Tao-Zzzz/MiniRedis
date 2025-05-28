package server

import (
	"bufio"                        // 缓冲读写库
	"log"                          // 日志库
	"MiniRedis/internal/protocol" // 导入我们自己写的 RESP 协议处理模块
	"MiniRedis/internal/store"    // 导入我们自己写的存储模块
	"net"                          // 提供网络通信能力（监听端口、处理连接等）
	"sync"                         // 提供同步原语，这里用的是 WaitGroup
)

// Server 代表MiniRedis服务器
type Server struct {
	listener net.Listener     // 监听客户端连接
	store    *store.Store     // 指向内部的键值存储结构体
	wg       sync.WaitGroup   // 用于等待所有客户端连接结束
}

// NewServer 创建一个新的服务器实例
func NewServer(addr string) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	// 成功,返回一个 Server 实例
	return &Server{
		listener: listener,
		store:    store.NewStore(), // 创建一个内部存储实例
	}, nil
}

// Start 启动服务器，接受客户端连接
func (s *Server) Start() error {
	for {
		select {
		case <-ctx.Done():
			s.listener.Close()
			return ctx.Err()
		default:
			conn, err := s.listener.Accept()// 接受一个新连接
			if err != nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return err
				}
			}
			s.wg.Add(1)
			// 为每个客户端连接启动一个goroutine
			go s.handleConnection(ctx, conn)// 启动一个 goroutine 处理该连接（协程）
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
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 读取并解析客户端发送的RESP命令
			cmd, err := protocol.ParseRESP(reader)
			if err != nil {
				if err.Error() == "EOF" {
					log.Printf("Client %s disconnected", conn.RemoteAddr())
					return
				}
				protocol.WriteError(conn, "ERR invalid command")
				continue
			}

			// 处理命令
			response := s.handleCommand(cmd)
			// 发送响应给客户端
			_, err = conn.Write([]byte(response))
			if err != nil {
				log.Printf("Error writing to client %s: %v", conn.RemoteAddr(), err)
				return
			}
		}
	}
}


// handleCommand 处理解析后的命令
func (s *Server) handleCommand(cmd *protocol.Command) string {
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
		s.store.Set(cmd.Args[0], cmd.Args[1], expire)
		return "+OK\r\n"
	case "GET":
		if len(cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'GET' command\r\n"
		}
		if value, exists := s.store.Get(cmd.Args[0]); exists {
			return protocol.SerializeBulkString(value)
		}
		return "$-1\r\n"
	case "DEL":
		if len(cmd.Args) < 1 {
			return "-ERR wrong number of arguments for 'DEL' command\r\n"
		}
		count := s.store.Delete(cmd.Args...)
		return protocol.SerializeInteger(count)
	case "EXISTS":
		if len(cmd.Args) < 1 {
			return "-ERR wrong number of arguments for 'EXISTS' command\r\n"
		}
		count := s.store.Exists(cmd.Args...)
		return protocol.SerializeInteger(count)
	case "EXPIRE":
		if len(cmd.Args) != 2 {
			return "-ERR wrong number of arguments for 'EXPIRE' command\r\n"
		}
		seconds, err := protocol.ParseInt(cmd.Args[1])
		if err != nil {
			return "-ERR invalid expire time\r\n"
		}
		if s.store.Expire(cmd.Args[0], seconds) {
			return ":1\r\n"
		}
		return ":0\r\n"
	case "LPUSH":
		if len(cmd.Args) < 2 {
			return "-ERR wrong number of arguments for 'LPUSH' command\r\n"
		}
		length := s.store.LPush(cmd.Args[0], cmd.Args[1:]...)
		return protocol.SerializeInteger(length)
	case "RPUSH":
		if len(cmd.Args) < 2 {
			return "-ERR wrong number of arguments for 'RPUSH' command\r\n"
		}
		length := s.store.RPush(cmd.Args[0], cmd.Args[1:]...)
		return protocol.SerializeInteger(length)
	case "LPOP":
		if len(cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'LPOP' command\r\n"
		}
		if value, exists := s.store.LPop(cmd.Args[0]); exists {
			return protocol.SerializeBulkString(value)
		}
		return "$-1\r\n"
	case "RPOP":
		if len(cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'RPOP' command\r\n"
		}
		if value, exists := s.store.RPop(cmd.Args[0]); exists {
			return protocol.SerializeBulkString(value)
		}
		return "$-1\r\n"
	case "LLEN":
		if len(cmd.Args) != 1 {
			return "-ERR wrong number of arguments for 'LLEN' command\r\n"
		}
		length := s.store.LLen(cmd.Args[0])
		return protocol.SerializeInteger(length)
	case "MULTI":
		if len(cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'MULTI' command\r\n"
		}
		s.store.BeginTransaction()
		return "+OK\r\n"
	case "EXEC":
		if len(cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'EXEC' command\r\n"
		}
		results := s.store.ExecuteTransaction()
		return protocol.SerializeArray(results)
	case "DISCARD":
		if len(cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'DISCARD' command\r\n"
		}
		s.store.DiscardTransaction()
		return "+OK\r\n"
	default:
		return "-ERR unknown command '" + cmd.Name + "'\r\n"
	}
}