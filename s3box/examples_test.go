package s3box

import (
	"encoding/json"
	"log"
	"time"
)

const (
	yourAWSAccessKeyID     = "Id"
	yourAWSSecretAccessKey = "Secret"
)

type Row struct {
	Time  time.Time
	Value string
}

type DataStore struct {
	count int
}

func (ds DataStore) Iter() bool {
	ds.count++
	if ds.count > 10 {
		return false
	}
	return true
}

func (ds DataStore) GetNextRow() Row {
	return Row{}
}

func getSomeDataStore() DataStore {
	return DataStore{}
}

func runSomeCustomCopyCommand(manifests []string) error {
	return nil
}

func handleError(err error) {
	log.Fatalf("Got an error: %s", err)
}

func ExampleS3BoxUsage() {
	// Setup
	sb, err := NewS3Box(NewS3BoxOptions{
		S3Bucket:    "bucket-with-user-access",
		AWSKey:      yourAWSAccessKeyID,
		AWSPassword: yourAWSSecretAccessKey,
	})
	handleError(err)

	// Data Transfer to s3
	dataStore := getSomeDataStore()
	for dataStore.Iter() {
		rowData := dataStore.GetNextRow() // Return a single Row object
		rowBytes, _ := json.Marshal(rowData)
		handleError(sb.Pack(rowBytes))
	}

	// Manifest creation and data transfer to Redshift
	manifestKey := "data_locations"
	nManifests := 2
	manifests, err := sb.CreateManifests(manifestKey, nManifests)
	handleError(err)
	handleError(runSomeCustomCopyCommand(manifests))
}
