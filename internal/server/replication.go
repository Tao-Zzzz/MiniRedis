package server

import (
	"context"
	"fmt"
	"log"
	"MiniRedis/internal/protocol"
	"MiniRedis/internal/store"
	"bufio"
	"net"
)

// Replication 管理主从复制
type Replication struct {
	masterAddr string         // 主服务器地址
	store      *store.Store   // 数据存储实例
	cmdCh      chan clientCommand // 命令通道（连接到服务器的事件循环）
	conn       net.Conn       // 与主服务器的连接
}

// NewReplication 创建复制实例
func NewReplication(masterAddr string, store *store.Store, cmdCh chan clientCommand) *Replication {
	
	return &Replication{
		masterAddr: masterAddr,
		store:      store, // 关联数据存储
		cmdCh:      cmdCh, // 连接到服务器的事件循环通道
	}
}

// Start 启动从服务器同步
func (r *Replication) Start(ctx context.Context) {
	// 连接到主服务器
	conn, err := net.Dial("tcp", r.masterAddr)
	if err != nil {
		log.Printf("Failed to connect to master: %v", err)
		return
	}
	r.conn = conn
	defer conn.Close()

	log.Println("Replication started") 
	
	// 从主服务器读取数据
	reader := bufio.NewReader(conn)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// 解析主服务器发送的命令
			cmd, err := protocol.ParseRESP(reader)
			if err != nil {
				log.Printf("Replication error: %v", err)
				return
			}

			// 提交命令到事件循环
			r.cmdCh <- clientCommand{
				clientID: "replication", // 使用固定的ID标识复制客户端
				cmd:      cmd,
				conn:     nil, // 不发送响应
			}
		}
	}
}

// SendCommand 发送命令到从服务器
func (r *Replication) SendCommand(cmd *protocol.Command) {
	if r.conn == nil {
		return
	}

	// 将命令转换为RESP格式并发送
	resp := fmt.Sprintf("*%d\r\n", len(cmd.Args)+1)
	resp += fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.Name), cmd.Name)
	for _, arg := range cmd.Args {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}

	// 发送命令到从服务器
	_, err := r.conn.Write([]byte(resp))
	if err != nil {
		log.Printf("Failed to send command to slave: %v", err)
		r.conn.Close()
		r.conn = nil
	}
}