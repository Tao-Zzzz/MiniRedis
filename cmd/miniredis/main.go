package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"MiniRedis/internal/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 解析命令行参数
	isSlave := flag.Bool("slave", false, "Run as slave server")
	masterAddr := flag.String("master", "", "Master server address (for slave mode)")
	flag.Parse()

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 初始化服务器
	addr := ":6379"
	if *isSlave {
		addr = ":6380"
	}
	srv, err := server.NewServer(addr, *isSlave, *masterAddr)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// 捕获系统信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 启动服务器
	fmt.Printf("MiniRedis server started on %s (slave: %v)\n", addr, *isSlave)
	go func() {
		if err := srv.Start(ctx); err != nil {
			log.Printf("Server stopped: %v", err)
		}
	}()

	// 等待信号
	<-sigCh
	fmt.Println("\nShutting down server...")
	cancel()
	srv.Wait()
	fmt.Println("Server stopped gracefully")
}