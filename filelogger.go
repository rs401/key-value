package main

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"sync"
)

type FileTransactionLogger struct {
	events       chan<- Event
	errors       <-chan error
	lastSequence uint64
	file         *os.File
	wg           *sync.WaitGroup
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	l.wg.Add(1)
	l.events <- Event{
		EventType: EventDelete,
		Key:       key,
	}
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.wg.Add(1)
	l.events <- Event{
		EventType: EventPut,
		Key:       key,
		Value:     value,
	}
}

func NewFileTransactionLogger(filename string) (*FileTransactionLogger, error) {
	var err error
	var l FileTransactionLogger = FileTransactionLogger{wg: &sync.WaitGroup{}}
	l.file, err = os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}

	return &l, nil
}

func (l *FileTransactionLogger) Run() {
	// Make a buffered events channel
	events := make(chan Event, 16)
	l.events = events
	// Make a buffered errors channel
	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		// Retrieve the next Event
		for e := range events {
			// Increment sequence number
			l.lastSequence++
			// Write the event to the log
			_, err := fmt.Fprintf(
				l.file,
				"%d\t%d\t%s\t%s\n",
				l.lastSequence, e.EventType, url.QueryEscape(e.Key), url.QueryEscape(e.Value))

			if err != nil {
				errors <- err
				return
			}
			l.wg.Done()
		}
	}()
} // END Run

func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file)
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		var e Event
		defer close(outEvent)
		defer close(outError)
		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s",
				&e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {

				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}
			var err error
			e.Key, err = url.QueryUnescape(e.Key)
			if err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}
			e.Value, err = url.QueryUnescape(e.Value)
			if err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}

			// Make sure the sequence is increasing
			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}

			// Update last used sequence #
			l.lastSequence = e.Sequence

			// Send the event along
			outEvent <- e
		}

		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()

	return outEvent, outError
} // END ReadEvents

func (l *FileTransactionLogger) Close() error {
	l.wg.Wait()
	// Close events chan
	if l.events != nil {
		close(l.events)
	}
	// Close transaction file
	return l.file.Close()
}

func (l *FileTransactionLogger) Wait() {
	l.wg.Wait()
}
