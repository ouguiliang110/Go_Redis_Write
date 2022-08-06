package database

import (
	"go_redis_write/interface/resp"
	"go_redis_write/resp/reply"
)

func Ping(db *DB, args [][]byte) resp.Reply {
	return reply.MakePongReply()
}

//特殊关键字，在这个包开始运行的时候就会被定义
func init() {
	RegisterCommand("ping", Ping, 1)
}
