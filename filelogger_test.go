package main

import (
	"os"
	"testing"
)

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func TestFileTransactionLogger_NewFileTransactionLogger(t *testing.T) {
	const filename = "/tmp/new-ft-logger.txt"
	defer os.Remove(filename)

	tl, err := NewFileTransactionLogger(filename)

	if tl == nil {
		t.Error("Logger is nil?")
	}

	if err != nil {
		t.Errorf("Got error: %v", err)
	}

	if !fileExists(filename) {
		t.Errorf("File %s doesn't exist", filename)
	}
}

func TestFileTransactionLogger_WritePut(t *testing.T) {
	const filename = "/tmp/write-put.txt"
	defer os.Remove(filename)

	tl, err := NewFileTransactionLogger(filename)
	if err != nil {
		t.Errorf("Got error: %v", err)
	}
	tl.Run()
	defer tl.Close()

	tests := []struct {
		key   string
		value string
	}{
		// TODO: Add test cases.
		{key: "write-put-key", value: "write-put-value-a"},
		{key: "write-put-key", value: "write-put-value-b"},
		{key: "write-put-key", value: "write-put-value-c"},
		{key: "write-put-key", value: "write-put-value-d"},
	}
	for _, tt := range tests {
		tl.WritePut(tt.key, tt.value)
	}
	tl.Wait()

	tlResult, err := NewFileTransactionLogger(filename)
	if err != nil {
		t.Errorf("Got error: %v", err)
	}
	defer tlResult.Close()

	events, errors := tlResult.ReadEvents()

	for event := range events {
		t.Log(event)
	}

	err = <-errors
	if err != nil {
		t.Error(err)
	}

	if tl.lastSequence != tlResult.lastSequence {
		t.Error("Sequence did not match.")
	}

}
