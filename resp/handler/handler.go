package handler

import (
	"context"
	"go_redis_write/cluster"
	"go_redis_write/config"
	_ "go_redis_write/config"
	"go_redis_write/database"
	databaseface "go_redis_write/interface/database"
	"go_redis_write/lib/logger"
	"go_redis_write/lib/sync/atomic"
	"go_redis_write/resp/connection"
	"go_redis_write/resp/parser"
	"go_redis_write/resp/reply"
	"io"
	"net"
	"strings"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

// RespHandler implements tcp.Handler and serves as a redis handler
type RespHandler struct {
	activeConn sync.Map // *client -> placeholder
	db         databaseface.Database
	closing    atomic.Boolean // refusing new client and new request
}

// MakeHandler creates a RespHandler instance
func MakeHandler() *RespHandler {
	var db databaseface.Database
	db = database.NewStandaloneDatabase() //实现一个回复的接口
	if config.Properties.Self != "" &&
		len(config.Properties.Peers) > 0 {
		db = cluster.MakeClusterDatabase()
	} else {
		db = database.NewStandaloneDatabase()
	}
	return &RespHandler{
		db: db,
	}
}

func (h *RespHandler) closeClient(client *connection.Connection) { //关闭单个客户端
	_ = client.Close()
	h.db.AfterClientClose(client) //对客户端关闭得善后处理内容
	h.activeConn.Delete(client)
}

// Handle receives and executes redis commands
func (h *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(conn) //交给解析器进行解析，表明其中已有一个协程开始工作了，有了channel后就可
	/*
		type Payload struct {
			Data resp.Reply
			Err error
		}
	*/
	//监听这几个解析器协程的情况，如果有的话可以进行处理
	for payload := range ch { //监听管道，其中管道里面主要存储的是	Data resp.Reply 相应的返回值
		if payload.Err != nil {
			if payload.Err == io.EOF || //就是连接出现错误，或者是客户端发送关闭
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol err,协议出错
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil { //如果Data是空的，则回空
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*reply.MultiBulkReply) //将Data转成多行
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}

		result := h.db.Exec(client, r.Args) //执行每个连接
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes) //位置错误
		}
	}
}

// Close stops handler
func (h *RespHandler) Close() error { //关闭全部客户端的操作
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	// TODO: concurrent wait
	h.activeConn.Range(func(key interface{}, val interface{}) bool { //遍历每一个可连接对象完成其关闭得内容
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})
	h.db.Close()
	return nil
}
