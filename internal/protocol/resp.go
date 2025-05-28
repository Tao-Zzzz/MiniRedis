package protocol

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Command 代表解析后的命令
type Command struct {
	Name string
	Args []string
}

// ParseRESP 解析RESP协议的命令
func ParseRESP(reader *bufio.Reader) (*Command, error) {
	// 读取一行数据, 以\n结尾
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	// 去掉两端空白字符\r,\n,空格
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "*") {// 检查前缀
		return nil, fmt.Errorf("invalid RESP array")
	}

	// 解析数组长度
	count, err := strconv.Atoi(line[1:])// 从下标1开始
	if err != nil {
		return nil, fmt.Errorf("invalid array length")
	}

	// 读取命令和参数
	args := make([]string, 0, count) // 容量为count的空切片
	for i := 0; i < count; i++ {
		// 再读取一行
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		// 去掉两端空白字符\r,\n,空格
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "$") {
			return nil, fmt.Errorf("invalid bulk string")
		}

		// 解析字符串长度
		length, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length")
		}

		// 读取字符串内容, 精确读取length + 2 的内容
		data := make([]byte, length+2) // 包括\r\n

		//io.ReadFull：确保读取足够字节，否则报错。
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return nil, err
		}
		// 将前面length个字节转换为字符串.不包括最后的\r\n
		args = append(args, string(data[:length]))
	}

	// 如果没有参数，返回错误
	if len(args) == 0 {
		return nil, fmt.Errorf("empty command")
	}

	return &Command{
		Name: strings.ToUpper(args[0]),
		Args: args[1:],
	}, nil
}

// SerializeBulkString 序列化字符串为RESP格式
func SerializeBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// SerializeInteger 序列化整数为RESP格式
func SerializeInteger(i int) string {
	return fmt.Sprintf(":%d\r\n", i)
}

// SerializeArray 序列化数组为RESP格式
func SerializeArray(arr []string) string {
	result := fmt.Sprintf("*%d\r\n", len(arr))
	for _, item := range arr {
		result += item
	}
	return result
}

// WriteError 写入错误响应
func WriteError(w io.Writer, msg string) {
	fmt.Fprintf(w, "-%s\r\n", msg)
}

// ParseInt 解析字符串为整数
func ParseInt(s string) (int, error) {
	return strconv.Atoi(s)
}