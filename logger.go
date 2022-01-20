package main

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error
	ReadEvents() (<-chan Event, <-chan error)
	Run()
	Close() error
}

type EventType byte

const (
	_                     = iota // 0
	EventDelete EventType = iota
	EventPut
)

type Event struct {
	Sequence  uint64    // A unique record ID
	EventType EventType // The action taken
	Key       string    // The key affected by this transaction
	Value     string    // The value of a PUT the transaction
}
