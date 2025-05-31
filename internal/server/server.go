package server

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"MiniRedis/internal/protocol"
	"MiniRedis/internal/pubsub"
	"MiniRedis/internal/store"
	"net"
	"sync"
)

// Server 代表MiniRedis服务器
type Server struct {
	listener      net.Listener
	slaveListener net.Listener
	store         *store.Store
	pubsub        *pubsub.PubSub
	isSlave       bool
	masterAddr    string
	replication   *Replication
	wg            sync.WaitGroup
	cmdCh         chan clientCommand
	subscribers   sync.Map
	slaves        sync.Map
}

// clientCommand 代表客户端命令
type clientCommand struct {
	clientID string
	cmd      *protocol.Command
	conn     net.Conn
}

// NewServer 创建服务器实例
func NewServer(addr string, isSlave bool, masterAddr string) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &Server{
		listener:    listener,
		store:       store.NewStore(),
		pubsub:      pubsub.NewPubSub(),
		isSlave:     isSlave,
		masterAddr:  masterAddr,
		cmdCh:       make(chan clientCommand, 100),
	}
	srv.store.SetPubSub(srv.pubsub)

	if isSlave && masterAddr != "" {
		srv.replication = NewReplication(masterAddr, srv.store, srv.cmdCh)
	} else {
		slaveAddr := ":6380"
		slaveListener, err := net.Listen("tcp", slaveAddr)
		if err != nil {
			listener.Close()
			return nil, err
		}
		srv.slaveListener = slaveListener
		srv.replication = &Replication{
			store: srv.store,
			cmdCh: srv.cmdCh,
		}
	}
	return srv, nil
}

// Start 启动服务器
func (s *Server) Start(ctx context.Context) error {
	go s.eventLoop(ctx)

	if s.isSlave && s.masterAddr != "" {
		go s.replication.Start(ctx)
	}
	if !s.isSlave {
		go s.handleSlaveConnections(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			s.listener.Close()
			if s.slaveListener != nil {
				s.slaveListener.Close()
			}
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

// Wait 等待所有连接关闭
func (s *Server) Wait() {
	s.wg.Wait()
	close(s.cmdCh)
}

// handleSlaveConnections 处理从服务器连接
func (s *Server) handleSlaveConnections(ctx context.Context) {
	if s.slaveListener == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			s.slaves.Range(func(key, value interface{}) bool {
				conn := value.(net.Conn)
				conn.Close()
				return true
			})
			return
		default:
			conn, err := s.slaveListener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					log.Printf("Error accepting slave connection: %v", err)
					continue
				}
			}
			slaveID := conn.RemoteAddr().String()
			s.slaves.Store(slaveID, conn)
			go s.replication.RegisterSlave(slaveID, conn)
		}
	}
}

// eventLoop 处理命令和Pub/Sub消息
func (s *Server) eventLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-s.cmdCh:
			response := s.handleCommand(cmd)
			if response != "" && cmd.conn != nil {
				_, err := cmd.conn.Write([]byte(response))
				if err != nil {
					log.Printf("Error writing to client %s: %v", cmd.clientID, err)
					s.pubsub.UnsubscribeAll(cmd.clientID)
					cmd.conn.Close()
				}
			}
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
	defer s.wg.Done()
	defer conn.Close()

	reader := bufio.NewReader(conn)
	clientID := conn.RemoteAddr().String()

	for {
		select {
		case <-ctx.Done():
			s.pubsub.UnsubscribeAll(clientID)
			s.subscribers.Delete(clientID)
			return
		default:
			if _, isSubscriber := s.subscribers.Load(clientID); isSubscriber {
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
				continue
			}
			cmd, err := protocol.ParseRESP(reader)
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
			fmt.Printf("Received command from %s: %s\n", clientID, cmd.Name)
			if cmd.Name == "SUBSCRIBE" {
				s.subscribers.Store(clientID, true)
			}
			s.cmdCh <- clientCommand{clientID: clientID, cmd: cmd, conn: conn}
		}
	}
}

// handleCommand 处理命令
func (s *Server) handleCommand(cmd clientCommand) string {
	clientID := cmd.clientID
    log.Printf("Handling command %s from client %s", cmd.cmd.Name, clientID)
    
    if s.isSlave && !isReadCommand(cmd.cmd.Name) && clientID != "replication" {
        log.Printf("Write command %s blocked on slave from client %s", cmd.cmd.Name, clientID)
        return "-ERR write commands not allowed on slave\r\n"
    }

	switch cmd.cmd.Name {
	case "SLAVEOF":
		if s.isSlave {
			return "-ERR SLAVEOF not allowed on slave\r\n"
		}
		if len(cmd.cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'SLAVEOF' command\r\n"
		}
		// 确认从服务器连接，响应 OK
		return "+OK\r\n"
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
		s.store.Set(clientID, cmd.cmd.Args[0], cmd.cmd.Args[1], expire)
		if !s.isSlave && s.replication != nil {
			s.replication.SendCommand(cmd.cmd)
		}
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
		s.store.BeginTransaction(clientID)
		return "+OK\r\n"
	case "EXEC":
		if len(cmd.cmd.Args) != 0 {
			return "-ERR wrong number of arguments for 'EXEC' command\r\n"
		}
		results := s.store.ExecuteTransaction(clientID)
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
		return ""
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