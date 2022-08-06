package database

import (
	"go_redis_write/interface/resp"
	"go_redis_write/lib/utils"
	"go_redis_write/lib/wildcard"
	"go_redis_write/resp/reply"
)

//DEL
//EXISTS
//KEYS
//FLUSDB
//TYPE
//RENAME
//RENAMENX

//DEL K1 K2 K3
// execDel removes a key from db
func execDel(db *DB, args [][]byte) resp.Reply {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	deleted := db.Removes(keys...) //删除多个key
	if deleted > 0 {
		db.addAof(utils.ToCmdLine2("del", args...))
	}
	return reply.MakeIntReply(int64(deleted)) //返回的是整数
}

//EXISTS K1 K2 K3
// execExists checks if a is existed in db
func execExists(db *DB, args [][]byte) resp.Reply {
	result := int64(0)
	for _, arg := range args {
		key := string(arg)
		_, exists := db.GetEntity(key)
		if exists {
			result++
		}
	}
	return reply.MakeIntReply(result)
}

//FLUSHDB 清空数据库
// execFlushDB removes all data in current db
func execFlushDB(db *DB, args [][]byte) resp.Reply {
	db.Flush()
	db.addAof(utils.ToCmdLine2("flushdb", args...))
	return &reply.OkReply{}
}

//TYPE k1
// execType returns the type of entity, including: string, list, hash, set and zset
func execType(db *DB, args [][]byte) resp.Reply {
	key := string(args[0])
	entity, exists := db.GetEntity(key)
	if !exists {
		return reply.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return reply.MakeStatusReply("string")
		//case *list.LinkedList:
		//    return reply.MakeStatusReply("list")
		//case dict.Dict:
		//    return reply.MakeStatusReply("hash")
		//case *set.Set:
		//    return reply.MakeStatusReply("set")
		//case *sortedset.SortedSet:
		//    return reply.MakeStatusReply("zset")
	}
	return &reply.UnKnownErrReply{}
}

//RENAME key1 key2  改一下key1的名称为key2
// execRename a key
func execRename(db *DB, args [][]byte) resp.Reply {
	if len(args) != 2 {
		return reply.MakeErrReply("ERR wrong number of arguments for 'rename' command")
	}
	src := string(args[0])
	dest := string(args[1])

	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}
	db.PutEntity(dest, entity) //添加dest
	db.Remove(src)             //删除src
	db.addAof(utils.ToCmdLine2("rename", args...))
	return &reply.OkReply{}
}

//RENAMENX k1 k2  新的k2要保证不存在
// execRenameNx a key, only if the new key does not exist
func execRenameNx(db *DB, args [][]byte) resp.Reply {
	src := string(args[0])
	dest := string(args[1])

	_, ok := db.GetEntity(dest)
	if ok { //如果这个键值存在的话，什么也不干
		return reply.MakeIntReply(0)
	}

	entity, ok := db.GetEntity(src)
	if !ok {
		return reply.MakeErrReply("no such key")
	}

	db.Removes(src, dest) // clean src and dest with their ttl
	db.PutEntity(dest, entity)
	//db.addAof(utils.ToCmdLine2("renamenx", args...))
	return reply.MakeIntReply(1)
}

/*
redis 127.0.0.1:6379> KEYS runoob*  比如查找所有匹配的通配符
1) "runoob3"
2) "runoob1"
3) "runoob2"
*/

// execKeys returns all keys matching the given pattern
func execKeys(db *DB, args [][]byte) resp.Reply {
	pattern := wildcard.CompilePattern(string(args[0]))
	result := make([][]byte, 0)
	db.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			result = append(result, []byte(key))
		}
		return true
	})
	return reply.MakeMultiBulkReply(result)
}

func init() {
	RegisterCommand("Del", execDel, -2) //最少两个，但是个数则需要-2 表示大于2
	RegisterCommand("Exists", execExists, -2)
	RegisterCommand("Keys", execKeys, 2)
	RegisterCommand("FlushDB", execFlushDB, -1) //FLUSHDB a, b, c
	RegisterCommand("Type", execType, 2)
	RegisterCommand("Rename", execRename, 3) //入参要三个
	RegisterCommand("RenameNx", execRenameNx, 3)
}
