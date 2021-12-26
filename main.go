package main

import (
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func helloGoHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	if name == "" {
		name = "gorilla mux"
	}
	greeting := "Hello " + name + "!\n"
	w.Write([]byte(greeting))
}

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
}

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
}

func main() {
	r := mux.NewRouter()
	// http.HandleFunc("/", helloGoHandler)
	r.HandleFunc("/v1/{key}", kvPutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", kvGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", kvDeleteHandler).Methods("DELETE")
	r.HandleFunc("/", helloGoHandler)
	r.HandleFunc("/{name}", helloGoHandler)

	log.Fatal(http.ListenAndServe(":8080", r))
}
