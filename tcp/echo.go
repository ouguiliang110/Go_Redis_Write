package tcp

import (
	"bufio"
	"context"
	"go_redis_write/lib/logger"
	"go_redis_write/lib/sync/atomic"
	"go_redis_write/lib/sync/wait"
	"io"
	"net"
	"sync"
)

type EchoClient struct { //相应的客户端
	Conn    net.Conn
	Waiting wait.Wait
}

func (e *EchoClient) Close() error {
	e.Waiting.WaitWithTimeout(10) //关闭的时候要等待10秒中，10秒后就会自动关闭
	_ = e.Conn.Close()
	return nil
	//panic("implement me")
}

func MakeHandler() *EchoHandler {
	return &EchoHandler{}
}

type EchoHandler struct { //相应请求
	activeConn sync.Map       //记录有多少个连接，其中装的是对客户端的连接信息
	closing    atomic.Boolean //关闭操作
}

func (handler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	//panic("implement me")
	if handler.closing.Get() { //读值的原子操作，如果是关闭的，则关闭请求
		_ = conn.Close()

	}
	client := &EchoClient{ //对这个客户端创建
		Conn: conn,
	}
	handler.activeConn.Store(client, struct{}{}) //空结构体不占用空间
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF { //EOF是数据的结束符
				logger.Info("Connecting close")
				handler.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		client.Waiting.Add(1) //就是写之间不要关掉我
		b := []byte(msg)
		_, _ = conn.Write(b)
		client.Waiting.Done() //写完后再关掉
	}
}

func (handler *EchoHandler) Close() error {
	//panic("implement me")
	logger.Info("handler shutting down")
	handler.closing.Set(true) //先设置为true，当有新的连接过来，直接关闭

	handler.activeConn.Range(func(key, value interface{}) bool { //功能是对Map里面每一个key进行关闭操作，
		client := key.(*EchoClient) //拿到客户端
		_ = client.Conn.Close()     //关闭
		return true                 //这个是对全部的key进行操作，如果是false则则是施加一次操作
	})
	return nil
}
