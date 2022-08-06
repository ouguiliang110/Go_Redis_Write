package database

import (
	"go_redis_write/datastruct/dict"
	"go_redis_write/interface/database"
	"go_redis_write/interface/resp"
	"go_redis_write/resp/reply"
	"strings"
)

// DB stores data and execute user's commands
type DB struct {
	index int //数据库ID
	// key -> DataEntity
	data   dict.Dict
	addAof func(CmdLine) //加上AOF方法
}

// ExecFunc is interface for command executor
// args don't include cmd line
type ExecFunc func(db *DB, args [][]byte) resp.Reply

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// makeDB create DB instance
func makeDB() *DB {
	db := &DB{
		data:   dict.MakeSyncDict(),
		addAof: func(line CmdLine) {},
	}
	return db
}

// Exec executes command within one database
func (db *DB) Exec(c resp.Connection, cmdLine [][]byte) resp.Reply {
	//PING SET SETNX
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName] //取指令给拿出来，然后执行这个函数
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "'")
	}
	//SET k
	if !validateArity(cmd.arity, cmdLine) { //要求的指令的个数,参数个数不对
		return reply.MakeArgNumErrReply(cmdName)
	}
	fun := cmd.executor
	//SET K V, K V
	return fun(db, cmdLine[1:]) //注意这个他们把其中的SET GET DEL等命令拿出来的
}

//SET K V 如果是固定的 arity = 3
//EXISTS k1 k2 k3 k4...  arity = -2 表示可以超过这个2
func validateArity(arity int, cmdArgs [][]byte) bool {
	argNum := len(cmdArgs)
	if arity >= 0 { //表明是定长的
		return argNum == arity
	}
	return argNum >= -arity //表明是变长的
}

/* ---- data Access ----- */

//在这个dict的基础重新在包上一层

// GetEntity returns DataEntity bind to given key
func (db *DB) GetEntity(key string) (*database.DataEntity, bool) {

	raw, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}
	entity, _ := raw.(*database.DataEntity)
	return entity, true
}

// PutEntity a DataEntity into DB
func (db *DB) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

// PutIfExists edit an existing DataEntity
func (db *DB) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

// PutIfAbsent insert an DataEntity only if the key not exists
func (db *DB) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

// Remove the given key from db
func (db *DB) Remove(key string) {
	db.data.Remove(key)
}

// Removes the given keys from db
func (db *DB) Removes(keys ...string) (deleted int) {
	deleted = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			deleted++
		}
	}
	return deleted
}

// Flush clean database
func (db *DB) Flush() {
	db.data.Clear()
}
