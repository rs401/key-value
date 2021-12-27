package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

var logWriter TransactionLogger

// https://blog.questionable.services/article/guide-logging-middleware-go/
// Thanks Matt Silverlock
func logMiddleware(next http.Handler) http.Handler {
	// We wrap our anonymous function, and cast it to a http.HandlerFunc
	// Because our function signature matches ServeHTTP(w, r), this allows
	// our function (type) to implicitly satisfy the http.Handler interface.
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			// Logic before - reading request values, putting things into the
			// request context, performing authentication

			// value, _ := io.ReadAll(r.Body)
			// Important that we call the 'next' handler in the chain. If we don't,
			// then request handling will stop here.
			next.ServeHTTP(w, r)
			// Logic after - useful for logging, metrics, etc.
			log.Printf(" %s ==> %s %s", r.Method, r.Host, r.URL)
			// log.Printf("\t==> %s", string(value))
		})
} // END logMiddleware

func initializeTransactionLog() error {
	var err error

	logWriter, err = NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}

	events, errors := logWriter.ReadEvents()
	e, ok := Event{}, true

	// If channel is closed, 'ok' will be false and end loop.
	// If 'err' is set to non-nil from errors chan or Put/Delete transactions,
	// the loop will end.
	for ok && err == nil {
		select {
		case err, ok = <-errors: // Retrieve any errors
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete: // Got a DELETE event!
				err = Delete(e.Key)
			case EventPut: // Got a PUT event!
				err = Put(e.Key, e.Value)
			}
		}
	}

	logWriter.Run()

	return err
} // END initializeTransactionLog

// PUT /v1/{key}
func kvPutHandler(w http.ResponseWriter, r *http.Request) {
	// Get the key from the request
	vars := mux.Vars(r)
	key := vars["key"]

	// Get the value from the body
	value, err := io.ReadAll(r.Body)
	// Check the error
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Store the key/value
	err = Put(key, string(value))
	// Check the error
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	logWriter.WritePut(key, string(value))
} // END kvPutHandler

// GET /v1/{key}
func kvGetHandler(w http.ResponseWriter, r *http.Request) {
	// Get vars
	vars := mux.Vars(r)
	// Get key
	key := vars["key"]
	// Get value for key
	value, err := Get(key)
	// Check error for not exist
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	// Check error
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Return value
	w.Write([]byte(value))
} // END kvGetHandler

// DELETE /v1/{key}
func kvDeleteHandler(w http.ResponseWriter, r *http.Request) {
	// Get vars
	vars := mux.Vars(r)
	// Get key
	key := vars["key"]
	// Get value for key
	_, err := Get(key)
	// Check error for not exist
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNoContent)
		return
	}
	// Check error
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// If we got here, key must exist
	// Delete key/value pair
	err = Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	logWriter.WriteDelete(key)
} // END kvDeleteHandler

func main() {
	err := initializeTransactionLog()
	if err != nil {
		panic(err)
	}
	r := mux.NewRouter()
	r.Use(logMiddleware)
	// http.HandleFunc("/", helloGoHandler)
	r.HandleFunc("/v1/{key}", kvPutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", kvGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", kvDeleteHandler).Methods("DELETE")

	log.Fatal(http.ListenAndServe(":8080", r))
}
