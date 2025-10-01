package main

type Transaction struct {
	cmds []*TxCommand
}

func NewTransaction() *Transaction {
	return &Transaction{}
}

type TxCommand struct {
	v       *Value
	handler Handler
}
