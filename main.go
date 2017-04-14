package main

import (
	"encoding/json"
	"github.com/couchbase/gocb"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

var bucket *gocb.Bucket // Couchbase bucket
var bucket_name string  // Couchbase bucket name

type Person struct { // Simple struct, represents data structure of the DB
	ID        string `json:"id,omitempty"`
	Firstname string `json:"first_name,omitempty"`
	Lastname  string `json:"last_name,omitempty"`
}

// Creates a new database entry with the given arguments.
// Arguments must send in the request body, not as a form.
func CreatePerson(w http.ResponseWriter, req *http.Request) {
	var new_person Person
	err := json.NewDecoder(req.Body).Decode(&new_person) // Take sent values and map them to the People struct
	if err != nil {                                      // Wrong values sent
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}

	query := gocb.NewN1qlQuery("SELECT `" + bucket_name + "`.* " +
		"FROM `" + bucket_name + "` WHERE id=$1")
	var n1qlParams []interface{}
	n1qlParams = append(n1qlParams, new_person.ID)

	rows, err := bucket.ExecuteN1qlQuery(query, n1qlParams) // Do a database query on given ID
	if err != nil {
		w.WriteHeader(401)
		w.Write([]byte(err.Error()))
		return
	}

	var exists Person
	rows.One(&exists)

	if exists != (Person{}) { // There is a entry in database. Can't insert
		w.WriteHeader(401)
		w.Write([]byte("Given ID exists"))
		return
	} else { // It's safe to insert
		_, err = bucket.Upsert(new_person.ID, new_person, 0)
		if err != nil {
			w.WriteHeader(400)
			w.Write([]byte(err.Error()))
		} else {
			w.WriteHeader(201) // Insertion success. Return inserted entry
			_ = json.NewEncoder(w).Encode(new_person)
		}
	}

}

// Returns the entry with the given ID.
// Returns empty JSON if nothing is found
func GetPerson(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get("id") // Get the ID in URL
	defer req.Body.Close()

	query := gocb.NewN1qlQuery("SELECT `" + bucket_name + "`.* " +
		"FROM `" + bucket_name + "` WHERE id=$1")
	var n1qlParams []interface{}
	n1qlParams = append(n1qlParams, id)
	rows, err := bucket.ExecuteN1qlQuery(query, n1qlParams) // Do a database query on given ID
	if err != nil {                                         // Something gone wrong
		w.WriteHeader(401)
		w.Write([]byte(err.Error()))
		return
	}

	// Return person in database
	// Else return empty JSON
	var person Person
	rows.One(&person)
	_ = json.NewEncoder(w).Encode(person)
}

// Removes the entry with the given ID.
func RemovePerson(w http.ResponseWriter, req *http.Request) {
	id := req.URL.Query().Get("id") // Get the ID in URL

	query := gocb.NewN1qlQuery("SELECT `" + bucket_name + "`.* " +
		"FROM `" + bucket_name + "` WHERE id=$1")
	var n1qlParams []interface{}
	n1qlParams = append(n1qlParams, id)
	rows, err := bucket.ExecuteN1qlQuery(query, n1qlParams) // Do a database query on given ID
	if err != nil {                                         // Something gone wrong while querying
		w.WriteHeader(401)
		w.Write([]byte(err.Error()))
		return
	}

	var person Person
	rows.One(&person)

	if person == (Person{}) { // Entry does not exists
		w.WriteHeader(401)
		w.Write([]byte("Enrty with given ID does not exists"))
		return
	} else {
		_, err := bucket.Remove(person.ID, 0) // Remove entry
		if err != nil {
			w.WriteHeader(401)
			w.Write([]byte(err.Error())) // Something gone wrong while removing
		} else {
			w.WriteHeader(200)
			w.Write([]byte("Deleted")) // Removed successfully
		}
	}
}

func main() {
	bucket_name = "people"
	cluster, _ := gocb.Connect("couchbase://YOUR-COUCHBASE-DATABASE-ADDRESS")
	bucket, _ = cluster.OpenBucket(bucket_name, "") // params: bucket-name, password
	router := mux.NewRouter()
	router.HandleFunc("/create", CreatePerson).Methods("POST")   // Create a new DB entry
	router.HandleFunc("/person", GetPerson).Methods("GET")          // Get entry with the given ID
	router.HandleFunc("/remove", RemovePerson).Methods("DELETE") // Delete entry with the given ID
	log.Println("Server started on port 12345")
	log.Fatal(http.ListenAndServe(":12345", router))
}
