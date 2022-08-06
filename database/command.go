package database

import "strings"

var cmdTable = make(map[string]*command) //记录所有的指令的结构体，等待去db.go中去执行

type command struct {
	executor ExecFunc //执行方式
	arity    int      //参数个数  比如SET K V的参数是三
}

func RegisterCommand(name string, exector ExecFunc, arity int) {
	name = strings.ToLower(name)
	cmdTable[name] = &command{
		executor: exector,
		arity:    arity,
	}
}
