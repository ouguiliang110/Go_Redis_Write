package database

import (
	"go_redis_write/interface/resp"
	"go_redis_write/resp/reply"
)

type EchoDatabase struct {
}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}
func (e EchoDatabase) Exec(client resp.Connection, args [][]byte) resp.Reply {
	return reply.MakeMultiBulkReply(args)
}

func (e EchoDatabase) AfterClientClose(c resp.Connection) {
	//panic("implement me")
}

func (e EchoDatabase) Close() {
	//panic("implement me")
}
