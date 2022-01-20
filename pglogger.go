package main

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	"github.com/lib/pq"
)

type PGTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
	wg     *sync.WaitGroup
}

type PGConfig struct {
	dbName   string
	host     string
	user     string
	password string
}

func NewPGTransactionLogger(config PGConfig) (TransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s dbname=%s user=%s password=%s sslmode=disable",
		config.host, config.dbName, config.user, config.password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	logger := &PGTransactionLogger{db: db, wg: &sync.WaitGroup{}}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %t", err)
	}
	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return logger, nil
}

func (l *PGTransactionLogger) verifyTableExists() (bool, error) {
	// db.exec query table exists
	rows, err := l.db.Query("SELECT 'transactions'::regclass;")
	if perr, ok := err.(*pq.Error); ok {
		if perr.Code == "42P01" {
			log.Println("42P01")
			return false, nil
		}
	}

	if err != nil {
		return false, err
	}
	defer rows.Close()
	var result string

	for rows.Next() && result != "transactions" {
		rows.Scan(&result)
	}

	return result == "transactions", rows.Err()
}

func (l *PGTransactionLogger) createTable() error {
	// db.exec query create table
	createTable := `CREATE TABLE transactions (
        sequence SERIAL PRIMARY KEY,
        event_type SMALLINT,
        key TEXT,
        value TEXT
    );`
	_, err := l.db.Exec(createTable)
	if err != nil {
		return err
	}
	return nil
}

func (l *PGTransactionLogger) WritePut(key, value string) {
	l.wg.Add(1)
	l.events <- Event{
		EventType: EventPut,
		Key:       key,
		Value:     value,
	}
	l.wg.Done()
}

func (l *PGTransactionLogger) WriteDelete(key string) {
	l.wg.Add(1)
	l.events <- Event{
		EventType: EventDelete,
		Key:       key,
	}
	l.wg.Done()
}

func (l *PGTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *PGTransactionLogger) Wait() {
	l.wg.Wait()
}

func (l *PGTransactionLogger) Close() error {
	l.wg.Wait()

	if l.events != nil {
		close(l.events)
	}

	return l.db.Close()
}

func (l *PGTransactionLogger) Run() {
	// Make a buffered events channel
	events := make(chan Event, 16)
	l.events = events
	// Make a buffered errors channel
	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		insertQuery := `INSERT INTO transactions(event_type, key, value)
            VALUES($1, $2, $3);`

		for e := range events {
			_, err := l.db.Exec(insertQuery, e.EventType, e.Key, e.Value)
			if err != nil {
				errors <- err
			}
		}
	}()
}

func (l *PGTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		defer close(outEvent)
		defer close(outError)
		query := `SELECT sequence, event_type, key, value FROM transactions
            ORDER BY sequence`
		rows, err := l.db.Query(query)
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}
		defer rows.Close()

		e := Event{}
		for rows.Next() {

			err = rows.Scan(
				&e.Sequence, &e.EventType,
				&e.Key, &e.Value)

			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err)
				return
			}

			outEvent <- e // Send e to the channel
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()
	return outEvent, outError
}
