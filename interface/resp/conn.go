package resp

type Connection interface {
	Write([]byte) error
	GetDBIndex() int
	SelectDB(int) //切DB情况
}
