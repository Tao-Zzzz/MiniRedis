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
	masterAddr := flag.String("master", "", "Master server address (for slave mode, e.g., 'localhost:6380')")
	addr := flag.String("addr", ":6379", "Server address to listen on (e.g., ':6379' for master, ':6381' for slave)")
	flag.Parse()

	// 验证参数
	if *isSlave && *masterAddr == "" {
		log.Fatal("Error: -master flag is required when running as slave")
	}

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 初始化服务器
	srv, err := server.NewServer(*addr, *isSlave, *masterAddr)
	if err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// 捕获系统信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 启动服务器
	fmt.Printf("MiniRedis server started on %s (slave: %v, master: %s)\n", *addr, *isSlave, *masterAddr)

	// 在新的goroutine中启动服务器
	go func() {
		if err := srv.Start(ctx); err != nil {
			if ctx.Err() == context.Canceled {
				log.Println("Server stopped due to context cancellation")
			} else {
				log.Printf("Server stopped with error: %v", err)
			}
		}
	}()

	// 等待信号
	<-sigCh
	fmt.Println("\nShutting down server...")

	// 取消上下文
	cancel()

	// 等待服务器停止
	srv.Wait()
	fmt.Println("Server stopped gracefully")
}