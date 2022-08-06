package database

import (
	"go_redis_write/aof"
	"go_redis_write/config"
	"go_redis_write/interface/resp"
	"go_redis_write/lib/logger"
	"go_redis_write/resp/reply"
	"strconv"
	"strings"
)

type StandaloneDatabase struct {
	dbSet      []*DB //默认是16个db
	aofHandler *aof.AofHandler
}

func NewStandaloneDatabase() *StandaloneDatabase { //初始化16个DB
	database := &StandaloneDatabase{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	database.dbSet = make([]*DB, config.Properties.Databases)
	for i := range database.dbSet {
		db := makeDB()
		db.index = i
		database.dbSet[i] = db
	}
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAOFHandler(database)
		if err != nil {
			panic(err)
		}
		database.aofHandler = aofHandler
		for _, db := range database.dbSet { //对每个db进行aof的初始化，可以让每个db完成aof的具体初始化功能 //存在闭包情况需要解决
			sdb := db
			sdb.addAof = func(line CmdLine) {
				database.aofHandler.AddAof(sdb.index, line)
			}
		}
	}
	return database
}

//select 1 切换为第一个数据库
func execSelect(c resp.Connection, database *StandaloneDatabase, args [][]byte) resp.Reply { //选择DB
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return reply.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(database.dbSet) {
		return reply.MakeErrReply("ERR DB index is out of range")
	}
	c.SelectDB(dbIndex)
	return reply.MakeOkReply()
}

//set k v
//get k
func (database *StandaloneDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	cmdName := strings.ToLower(string(args[0]))
	if cmdName == "select" {
		if len(args) != 2 {
			return reply.MakeArgNumErrReply("select ")
		}
		return execSelect(client, database, args[1:])
	}
	dbIndex := client.GetDBIndex()
	db := database.dbSet[dbIndex]
	return db.Exec(client, args)
}

func (database *StandaloneDatabase) AfterClientClose(c resp.Connection) {
	panic("implement me")
}

func (database *StandaloneDatabase) Close() {
	panic("implement me")
}
