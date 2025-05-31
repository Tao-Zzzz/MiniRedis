package server

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"MiniRedis/internal/protocol"
	"MiniRedis/internal/store"
	"net"
	"sync"
	"time"
)

// Replication 管理主从复制
type Replication struct {
	masterAddr string
	store      *store.Store
	cmdCh      chan clientCommand
	conn       net.Conn
	slaves     sync.Map // 存储从服务器连接
}

// NewReplication 创建复制实例
func NewReplication(masterAddr string, store *store.Store, cmdCh chan clientCommand) *Replication {
	return &Replication{
		masterAddr: masterAddr,
		store:      store,
		cmdCh:      cmdCh,
	}
}

// Start 启动从服务器同步
func (r *Replication) Start(ctx context.Context) {
	log.Printf("Attempting to connect to master at %s", r.masterAddr)
    var conn net.Conn
    var err error
    for i := 0; i < 5; i++ { // 尝试 5 次
        conn, err = net.Dial("tcp", r.masterAddr)
        if err == nil {
            break
        }
        log.Printf("Failed to connect to master at %s: %v, retrying (%d/5)", r.masterAddr, err, i+1)
        time.Sleep(2 * time.Second)
        select {
        case <-ctx.Done():
            log.Printf("Replication stopped due to context cancellation")
            return
        default:
        }
    }
    if err != nil {
        log.Printf("Failed to connect to master at %s after retries: %v", r.masterAddr, err)
        return
    }
    r.conn = conn
    defer conn.Close()
    log.Printf("Successfully connected to master at %s", r.masterAddr)

    // 发送 SLAVEOF 命令标识为从服务器
    _, err = conn.Write([]byte("*1\r\n$7\r\nSLAVEOF\r\n"))
    if err != nil {
        log.Printf("Failed to send SLAVEOF to master: %v", err)
        return
    }
    log.Printf("Sent SLAVEOF command to master")

    reader := bufio.NewReader(conn)
    // 读取 SLAVEOF 响应
    response, err := reader.ReadString('\n')
    if err != nil {
        log.Printf("Failed to read SLAVEOF response from master: %v", err)
        return
    }
    log.Printf("Received SLAVEOF response from master: %s", response)
    if response != "+OK\r\n" {
        log.Printf("Unexpected SLAVEOF response: %s", response)
        return
    }

    for {
        select {
        case <-ctx.Done():
            log.Printf("Replication stopped due to context cancellation")
            return
        default:
            cmd, err := protocol.ParseRESP(reader)
            if err != nil {
                log.Printf("Replication error: %v", err)
                return
            }
            log.Printf("Slave received command from master: %s (args: %v)", cmd.Name, cmd.Args)
            r.cmdCh <- clientCommand{
                clientID: "replication",
                cmd:      cmd,
                conn:     nil,
            }
        }
    }
}

// RegisterSlave 注册从服务器连接
func (r *Replication) RegisterSlave(slaveID string, conn net.Conn) {
    r.slaves.Store(slaveID, conn)
    reader := bufio.NewReader(conn)
    cmd, err := protocol.ParseRESP(reader)
    if err != nil || cmd.Name != "SLAVEOF" {
        log.Printf("Invalid slave connection %s: %v", slaveID, err)
        r.slaves.Delete(slaveID)
        conn.Close()
        return
    }
    // 响应 SLAVEOF 命令
    _, err = conn.Write([]byte("+OK\r\n"))
    if err != nil {
        log.Printf("Failed to respond to SLAVEOF for slave %s: %v", slaveID, err)
        r.slaves.Delete(slaveID)
        conn.Close()
        return
    }
    // 初始同步：发送当前所有键值对
    r.syncSlave(slaveID, conn)
    // 保持连接，持续读取从服务器的输入（可选，防止连接空闲）
    for {
        _, err := reader.ReadString('\n')
        if err != nil {
            log.Printf("Slave %s disconnected or error: %v", slaveID, err)
            r.slaves.Delete(slaveID)
            conn.Close()
            return
        }
    }
}

// syncSlave 同步当前存储状态到从服务器
func (r *Replication) syncSlave(slaveID string, conn net.Conn) {
	keys := r.store.GetAllKeys()
	for _, key := range keys {
		if value, exists := r.store.Get("replication", key); exists {
			cmd := &protocol.Command{
				Name: "SET",
				Args: []string{key, value},
			}
			resp := fmt.Sprintf("*%d\r\n", len(cmd.Args)+1)
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.Name), cmd.Name)
			for _, arg := range cmd.Args {
				resp += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
			}
			_, err := conn.Write([]byte(resp))
			if err != nil {
				log.Printf("Failed to sync key %s to slave %s: %v", key, slaveID, err)
				r.slaves.Delete(slaveID)
				return
			}
		}
	}
}

// SendCommand 发送命令到从服务器
func (r *Replication) SendCommand(cmd *protocol.Command) {
	resp := fmt.Sprintf("*%d\r\n", len(cmd.Args)+1)
	resp += fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.Name), cmd.Name)
	for _, arg := range cmd.Args {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	log.Printf("Master sending command to slaves: %s", cmd.Name)
	r.slaves.Range(func(key, value interface{}) bool {
		conn := value.(net.Conn)
		_, err := conn.Write([]byte(resp))
		if err != nil {
			log.Printf("Failed to send command to slave %s: %v", key, err)
			conn.Close()
			r.slaves.Delete(key)
		}
		return true
	})
}