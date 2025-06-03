# MiniRedis
本项目为一个golang和redis的学习项目, 目的是了解go语言和探索redis的运行逻辑

**涵盖TCP服务器、键值存储、RESP协议解析、AOF/RDB持久化、事务、发布/订阅、键空间通知和LRU淘汰等功能**
## 运行截图
### 键值存储, 事务
![[kid/Pasted image 20250530162948.png]]
### 订阅
![[kid/Pasted image 20250530164251.png]]
### 主从服务器
![[kid/Pasted image 20250601110509.png]]
## RedisServer 核心运行流程
### **main 创建 NewServer**  
main 解析 flag 参数（地址、是否从服务器、主服务器地址），调用 NewServer 初始化资源：TCP 监听器（单一端口，默认 :6379）、存储 store、发布/订阅 pubsub、命令通道 cmdCh、从服务器记录 slaves 和复制管理 replication，然后调用 Start。
### **Start 循环接收连接**  
<font color="#ff0000">Start 启动协程运行 eventLoop 处理命令</font>，若是**从服务器则启动协程连接主服务**器。循环调用 listener.Accept 接收客户端和从服务器连接，创建新协程调用 handleConnection 处理。
### **handleConnection 读取和解析数据**  
循环读取客户端发送的 RESP 协议数据，调用 protocol.ParseRESP 解析为命令。**若命令是 SLAVEOF，标记为从服务器并注册**；若命令是 SUBSCRIBE，标记为订阅者。提交命令到 cmdCh 通道。
### **eventLoop 处理命令**  
循环从 cmdCh 读取命令，调用 handleCommand 处理，获取响应并写入连接。若客户端是订阅者，推送 pubsub 消息。可扩展，命令异步处理。
### **handleCommand 操作 store**  
**根据命令名操作 store**：SET 设置键值，GET 获取值，DEL 删除键，LPUSH/RPUSH 操作列表等。主服务器转发**写命令给从服务器，返回 RESP 格式响应**。
## 主从复制
### **从服务器连接主服务器**  
从服务器启动时，replication.Start 建立与主服务器的 TCP 连接（同一端口，如 :6379），**发送 SLAVEOF 命令**，接收主服务器的初始数据和后续命令。
### **主服务器转发写操作**  
主服务器在 handleConnection **识别 SLAVEOF，注册从服务器连接，发送所有键值对进行初始同步。**写操作（如 SET, DEL）执行后，调用 replication.SendCommand 发送命令到从服务器。
### **从服务器拒绝写操作**  
在 handleCommand 中，若是从服务器且命令**非读操作（如 GET, LLEN），拒绝执行**，返回错误，确保数据由主服务器同步。
## PubSub订阅系统
一个读写锁
一个二级map, 一级键存储频道名, 二级键存储客户端id
一个map存储客户端的chan

订阅时注册map, 进行配置
**当要发送消息时, 调用Publish, publish内部将message传给频道中每一个订阅者 (客户端) 的chan(通道)中**,

**如果是订阅者客户端, 那么服务器查询该频道是否有消息待推送**
## 主从复制
从服务器建立的时候**增加一个与主服务器的连接**, 接收主服务器的命令, 主服务器进行写操作的时候发送命令到从服务器, 让从服务器进行一样的操作

而且在命令处理的地方, 如果是从无服务器则拒绝一切写操作

## flag配置启动参数
## [[chan]]实现goroutine间通信
有缓冲则异步, 没有则阻塞
配合select可以等待多个通道操作
有工作队列, 广播通知, 请求响应, 流水线, 扇入扇出几种经典模式
## RESP协议
[[ParseRESP]], [[redis常见操作]]
[[go的select]]
sync. **WaitGruop通过计数**的方法跟踪还有多少个协程没有处理完
## AOF
持久化,记录每个写操作
当创建Store实例时, 会优先加载AOF, 再加载RDB
## 快照RDB
Store初始化的时候起一个协程, 每分钟进行一次快照
保存快照将内存中的数据结构写入磁盘,
- **写入（序列化）**  
    先打开文件，然后用 [gob.NewEncoder(f)]创建编码器，依次调用 [encoder.Encode(data)]、[encoder.Encode(lists)]，把数据写入文件。  
    这些数据会**按顺序**写入文件（先 data，再 lists，再 expire）。
    
- **读取（反序列化）**  
    读取时也是先打开文件，用 [gob.NewDecoder(f)] 创建解码器，然后依次 [decoder.Decode(&data)]、[decoder.Decode(&lists)]、[decoder.Decode(&expire)]
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

