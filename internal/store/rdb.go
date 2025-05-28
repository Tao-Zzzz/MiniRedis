package store

import (
	"encoding/gob"
	"os"
	"time"
)

// Snapshot 保存数据到RDB文件
func Snapshot(data map[string]string, lists map[string][]string, expire map[string]time.Time, filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := gob.NewEncoder(f)
	if err := encoder.Encode(data); err != nil {
		return err
	}
	if err := encoder.Encode(lists); err != nil {
		return err
	}
	if err := encoder.Encode(expire); err != nil {
		return err
	}
	return nil
}

// LoadRDB 从RDB文件加载数据
func LoadRDB(filename string) (map[string]string, map[string][]string, map[string]time.Time, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, nil, nil, err
	}
	defer f.Close()

	data := make(map[string]string)
	lists := make(map[string][]string)
	expire := make(map[string]time.Time)

	decoder := gob.NewDecoder(f)
	if err := decoder.Decode(&data); err != nil {
		return nil, nil, nil, err
	}
	if err := decoder.Decode(&lists); err != nil {
		return nil, nil, nil, err
	}
	if err := decoder.Decode(&expire); err != nil {
		return nil, nil, nil, err
	}
	return data, lists, expire, nil
}