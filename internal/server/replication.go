package server

import (
	"bufio"           // 带缓冲的I/O操作
	"context"         // 上下文管理，用于处理取消信号
	"fmt"             // 格式化I/O
	"log"             // 日志记录
	"MiniRedis/internal/protocol" // 协议处理模块
	"MiniRedis/internal/store"    // 数据存储
	"net"             // 网络操作
	"sync"            // 同步原语，用于并发控制
	"time"            // 时间操作
)

// Replication 管理主从复制
type Replication struct {
	masterAddr string         // 主服务器地址（如果当前是从服务器）
	store      *store.Store   // 数据存储实例
	cmdCh      chan clientCommand // 命令处理通道
	conn       net.Conn       // 连接主服务器的连接对象（从服务器使用）
	slaves     sync.Map       // 并发安全映射，存储从服务器连接（主服务器使用）
}

// NewReplication 创建新的复制管理器
func NewReplication(masterAddr string, store *store.Store, cmdCh chan clientCommand) *Replication {
	return &Replication{
		masterAddr: masterAddr, // 设置主服务器地址
		store:      store,      // 设置数据存储
		cmdCh:      cmdCh,      // 设置命令通道
	}
}

// Start 启动从服务器同步流程（在从服务器上调用）
func (r *Replication) Start(ctx context.Context) {
	// 日志记录尝试连接主服务器
	log.Printf("Attempting to connect to master at %s", r.masterAddr)
	
	var conn net.Conn
	var err error
	
	// 重试机制：最多尝试5次连接
	for i := 0; i < 5; i++ {
		// 建立TCP连接到主服务器
		conn, err = net.Dial("tcp", r.masterAddr)
		if err == nil {
			break // 连接成功，退出循环
		}
		
		// 记录失败日志
		log.Printf("Failed to connect to master at %s: %v, retrying (%d/5)", r.masterAddr, err, i+1)
		
		// 等待2秒后重试
		time.Sleep(2 * time.Second)
		
		// 检查上下文是否已取消
		select {
		case <-ctx.Done():
			log.Printf("Replication stopped due to context cancellation")
			return
		default:
		}
	}
	
	// 检查是否成功连接
	if err != nil {
		log.Printf("Failed to connect to master at %s after retries: %v", r.masterAddr, err)
		return
	}
	
	// 保存连接并确保函数退出时关闭
	r.conn = conn
	defer conn.Close()
	log.Printf("Successfully connected to master at %s", r.masterAddr)

	// 发送SLAVEOF命令标识为从服务器（RESP格式）
	_, err = conn.Write([]byte("*1\r\n$7\r\nSLAVEOF\r\n"))
	if err != nil {
		log.Printf("Failed to send SLAVEOF to master: %v", err)
		return
	}
	log.Printf("Sent SLAVEOF command to master")

	// 创建带缓冲的读取器
	reader := bufio.NewReader(conn)
	
	// 读取主服务器对SLAVEOF的响应
	response, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Failed to read SLAVEOF response from master: %v", err)
		return
	}
	log.Printf("Received SLAVEOF response from master: %s", response)
	
	// 检查响应是否正确（应为"+OK\r\n"）
	if response != "+OK\r\n" {
		log.Printf("Unexpected SLAVEOF response: %s", response)
		return
	}

	// 主循环：处理来自主服务器的命令
	for {
		select {
		case <-ctx.Done(): // 上下文取消时停止
			log.Printf("Replication stopped due to context cancellation")
			return
		default:
			// 解析RESP格式的命令
			cmd, err := protocol.ParseRESP(reader)
			if err != nil {
				log.Printf("Replication error: %v", err)
				return
			}
			
			// 记录接收到的命令
			log.Printf("Slave received command from master: %s (args: %v)", cmd.Name, cmd.Args)
			
			// 将命令发送到命令通道进行处理（使用特殊clientID标识）
			r.cmdCh <- clientCommand{
				clientID: "replication", // 特殊标识复制来源
				cmd:      cmd,          // 解析后的命令
				conn:     nil,          // 无直接连接（因为是从服务器端）
			}
		}
	}
}

// RegisterSlave 注册从服务器连接（在主服务器上调用）
func (r *Replication) RegisterSlave(slaveID string, conn net.Conn) {
	// 存储从服务器连接
	r.slaves.Store(slaveID, conn)
	
	// 响应SLAVEOF命令（返回"+OK\r\n"）
	_, err := conn.Write([]byte("+OK\r\n"))
	if err != nil {
		// 写失败处理
		log.Printf("Failed to respond to SLAVEOF for slave %s: %v", slaveID, err)
		r.slaves.Delete(slaveID) // 从映射中删除
		conn.Close()             // 关闭连接
		return
	}
	
	// 执行初始同步：发送当前所有键值对
	r.syncSlave(slaveID, conn)
	
	// 保持连接活跃：持续读取输入（防止连接空闲断开）
	reader := bufio.NewReader(conn)
	for {
		// 简单读取一行（忽略内容）
		_, err := reader.ReadString('\n')
		if err != nil {
			// 连接断开或出错时处理
			log.Printf("Slave %s disconnected or error: %v", slaveID, err)
			r.slaves.Delete(slaveID) // 从映射中删除
			conn.Close()             // 关闭连接
			return
		}
	}
}

// syncSlave 同步当前存储状态到从服务器（主服务器操作）
func (r *Replication) syncSlave(slaveID string, conn net.Conn) {
	// 获取所有键
	keys := r.store.GetAllKeys()
	
	// 遍历所有键并同步
	for _, key := range keys {
		// 获取键值（使用特殊clientID）
		if value, exists := r.store.Get("replication", key); exists {
			// 构造SET命令
			cmd := &protocol.Command{
				Name: "SET",          // 命令类型
				Args: []string{key, value}, // 键值参数
			}
			
			// 创建RESP格式的响应
			resp := fmt.Sprintf("*%d\r\n", len(cmd.Args)+1)      // 参数数量（包括命令名）
			resp += fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.Name), cmd.Name) // 命令名
			// 添加所有参数
			for _, arg := range cmd.Args {
				resp += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
			}
			
			// 发送命令到从服务器
			_, err := conn.Write([]byte(resp))
			if err != nil {
				// 发送失败处理
				log.Printf("Failed to sync key %s to slave %s: %v", key, slaveID, err)
				r.slaves.Delete(slaveID) // 从映射中删除
				return
			}
		}
	}
}

// SendCommand 发送命令到所有从服务器（主服务器操作）
func (r *Replication) SendCommand(cmd *protocol.Command) {
	// 构建RESP格式的命令字符串
	resp := fmt.Sprintf("*%d\r\n", len(cmd.Args)+1)      // 参数数量（包括命令名）
	resp += fmt.Sprintf("$%d\r\n%s\r\n", len(cmd.Name), cmd.Name) // 命令名
	// 添加所有参数
	for _, arg := range cmd.Args {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	
	// 日志记录发送命令
	log.Printf("Master sending command to slaves: %s", cmd.Name)
	
	// 遍历所有从服务器发送命令
	r.slaves.Range(func(key, value interface{}) bool {
		// 类型断言为net.Conn
		conn := value.(net.Conn)
		
		// 发送命令
		_, err := conn.Write([]byte(resp))
		if err != nil {
			// 发送失败处理
			log.Printf("Failed to send command to slave %s: %v", key, err)
			conn.Close()             // 关闭连接
			r.slaves.Delete(key)     // 从映射中删除
		}
		return true // 继续遍历
	})
}