package main

import (
	"fmt"
	"log"
	"MiniRedis/internal/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 创建上下文，用于优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 初始化服务器，监听在6379端口
	srv, err := server.NewServer(":6379")
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// 捕获系统信号以优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 启动服务器
	fmt.Println("MiniRedis server started on :6379")
	go func() {
		if err := srv.Start(ctx); err != nil {
			log.Printf("Server stopped: %v", err)
		}
	}()

	// 等待信号
	<-sigCh
	fmt.Println("\nShutting down server...")
	cancel() // 触发上下文取消
	srv.Wait() // 等待所有连接关闭
	fmt.Println("Server stopped gracefully")
}