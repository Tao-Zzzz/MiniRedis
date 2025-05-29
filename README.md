# MiniRedis
本项目为一个golang和redis的学习项目, 旨在深刻理解golang与redis的运用等

涵盖TCP服务器、键值存储、RESP协议解析、AOF/RDB持久化、事务、发布/订阅、键空间通知和LRU淘汰等功能

## 运行流程
main创建NewServer, 然后Start,
Start循环中接收客户端连接, 创新新的协程handleConnection对连接进行处理
handleConnection中读取客户端发送的数据, 并进行解析, 然后将具体的任务发送给handleCommand处理
handleCommand对自创的store进行处理即可, 然后返回响应

## RESP协议
[[ParseRESP]], [[redis常见操作]]
[[go的select]]
sync. WaitGruop通过计数的方法跟踪还有多少个协程没有处理完
## 快照
Store初始化的时候起一个协程, 每分钟进行一次快照
保存快照将内存中的数据结构写入磁盘,
- **写入（序列化）**  
    先打开文件，然后用 [gob.NewEncoder(f)](vscode-file://vscode-app/c:/Microsoft%20VS%20Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html) 创建编码器，依次调用 [encoder.Encode(data)](vscode-file://vscode-app/c:/Microsoft%20VS%20Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)、[encoder.Encode(lists)](vscode-file://vscode-app/c:/Microsoft%20VS%20Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)、[encoder.Encode(expire)](vscode-file://vscode-app/c:/Microsoft%20VS%20Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)，把数据写入文件。  
    这些数据会**按顺序**写入文件（先 data，再 lists，再 expire）。
    
- **读取（反序列化）**  
    读取时也是先打开文件，用 [gob.NewDecoder(f)](vscode-file://vscode-app/c:/Microsoft%20VS%20Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html) 创建解码器，然后依次 [decoder.Decode(&data)](vscode-file://vscode-app/c:/Microsoft%20VS%20Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)、[decoder.Decode(&lists)](vscode-file://vscode-app/c:/Microsoft%20VS%20Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)、[decoder.Decode(&expire)](vscode-file://vscode-app/c:/Microsoft%20VS%20Code/resources/app/out/vs/code/electron-sandbox/workbench/workbench.html)。  
    读取顺序**必须和写入顺序一致**，否则会出错。

只要写和读的顺序一致, 那么久能够正确还原数据
## redis订阅事件
允许客户端订阅数据库中的键操作事件
Keyspace 事件是 Redis 在键被修改时生成的通知，包括：

- 键的创建、修改、删除
- 键的过期事件
- 键的驱逐事件（当内存不足被淘汰时）

## 其他
协程是轻量级线程, 运行在用户态, 无需上下文切换
**redis-cli -p 6380**
context 管理多个goroutine的生命周期
	context对象包含一个done通道
cancel () 关闭context的done通道, 这样的话就会发送context的done信号

	lists        map[string][]string       // 列表数据
	expire       map[string]time.Time      // 过期时间
	transactions map[*sync.RWMutex][]Command // 事务队列
map里是键, 外面是值, ==[]string==则又是一个数组 (切片)

go的指针就是智能指针
- Redis/MiniRedis 的事务（MULTI/EXEC）只会把“写操作”加入队列，读操作直接返回当前值。
- 
### ctrl c 关闭时序图
![[kid/Pasted image 20250529112358.png]]
## 语法
go不需要显示解引用

- `net.Listener`：接口类型，表示 TCP 监听器，由 `net.Listen` 返回。
    
- `*store.Store`：指向其他包中定义的结构体指针。
    
- `sync.WaitGroup`：类似 C++ 中的线程计数器或信号量，控制协程数量。
- 返回值 `(*Server, error)`：
- 多返回值是 Go 的特性，常见组合是 `(结果, 错误)`
- `&Server{...}`：创建一个结构体指针
- `go`：启动 goroutine（轻量线程）。是并发的基础。
- `defer`：注册延迟执行语句，在<font color="#ff0000">函数结束时</font>自动执行，类似析构。
- 顺序是 **后注册的先执行**（LIFO）。
-  `bufio.NewReader`：返回一个缓冲读取器。适用于流式数据读取。
- `io.ReadFull`：确保读取固定长度数据，否则出错。
- - `time.Time`：时间类型。
- `sync.RWMutex`：读写锁，支持并发读取，写入时加独占锁。
- `*os.File`：指向打开的文件对象。
- `os.OpenFile`：打开或创建文件。
	- `os.O_APPEND`：追加写。
	- `os.O_CREATE`：不存在则创建。
	- `os.O_WRONLY`：写模式。
	- `0644`：Unix 权限（rw-r--r--）。
- - `Lock/Unlock`：写锁，防止并发写冲突。
- `defer`：延迟执行，在函数返回前自动释放锁。
- TrimSpace去空格,\r和\n, 两边的
    

go

