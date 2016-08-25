package redbox

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

const (
	s3Bucket     = "test-bucket"
	awsKey       = "Key"
	awsPassword  = "Pass"
	testEndpoint = "testendpoint"
)

var (
	partialConfig = DestinationConfig{
		Schema: "test",
		Table:  "incomplete",
	}
	completeConfig = DestinationConfig{
		Schema: "test",
		Table:  "complete",
		Columns: []Column{
			Column{Name: "time", Type: "timestamp"},
			Column{Name: "id", Type: "text", DistKey: true},
		},
		DataTimestampColumn: "time",
	}
)

func getRegionForBucketSuccess(bucket string) (string, error) {
	return "Success", nil
}

func getRegionForBucketFail(bucket string) (string, error) {
	return "", fmt.Errorf("Failed getting bucket location.")
}

func writeToS3Success(s3Handler *s3.S3, schema, table string, input []byte) error {
	return nil
}

func writeToS3Fail(s3Handler *s3.S3, schema, table string, input []byte) error {
	return fmt.Errorf("Failed writing to s3.")
}

// Simulate a fast http call. We never care about the result.
func fastHandler(w http.ResponseWriter, r *http.Request) {
}

// Simulate a slow http call. We never care about the result.
func slowHandler(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Second)
}

func TestMain(m *testing.M) {
	// Assume successful s3 calls by default
	getRegionForBucket = getRegionForBucketSuccess
	writeToS3 = writeToS3Success

	os.Exit(m.Run())
}

func TestSuccessfulPipeCreation(t *testing.T) {
	assert := assert.New(t)
	// We should be able to successfully create a pipe with both complete and incomplete configurations.
	_, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &partialConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
	})
	assert.NoError(err)

	_, err = NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		JobEndpoint:       testEndpoint,
	})
	assert.NoError(err)
}

func TestUnsuccessfulPipeCreation(t *testing.T) {
	assert := assert.New(t)

	// Error with incomplete input
	_, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &partialConfig,
	})
	assert.Equal(err, ErrIncompleteArgs)

	// Error if we include a config without either a schema or table
	_, err = NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &DestinationConfig{Schema: "incomplete"},
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
	})
	assert.Equal(err, ErrIncompleteDestinationConfig)

	_, err = NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &DestinationConfig{Table: "incomplete"},
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
	})
	assert.Equal(err, ErrIncompleteDestinationConfig)

	// If we can't get the bucket location we should error
	getRegionForBucket = getRegionForBucketFail
	defer func() {
		getRegionForBucket = getRegionForBucketSuccess
	}()
	_, err = NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &partialConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
	})
	assert.Error(err)
}

func TestValidPacks(t *testing.T) {
	assert := assert.New(t)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &partialConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
	})
	assert.NoError(err)

	data1, _ := json.Marshal(map[string]interface{}{"Table": "row"})
	assert.NoError(rp.Pack(data1))
	assert.Equal(len(rp.bufferedData), len(data1)+1) // Account for the appended new line character

	rp, err = NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &partialConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
	})
	assert.NoError(err)

	data2, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(rp.Pack(data2))
	assert.Equal(len(rp.bufferedData), len(data2)+1) // Account for the appended new line character
}

func TestCorrectNumberOfS3Writes(t *testing.T) {
	assert := assert.New(t)
	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &partialConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		BufferSize:        len(data), // This is chosen such that each pack will overflow the buffer and "write" to s3
	})
	assert.NoError(err)

	nFiles := 10
	for i := 0; i < nFiles; i++ {
		assert.NoError(rp.Pack(data))
	}
	assert.Equal(rp.fileNumber, nFiles)
	assert.Equal(len(rp.fileLocations), nFiles)
}

func TestInvalidPacks(t *testing.T) {
	assert := assert.New(t)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		JobEndpoint:       testEndpoint,
	})
	assert.NoError(err)

	stringData := []byte("Some string")
	assert.Error(rp.Pack(stringData))

	jsonArray := []byte("[{\"k1\": \"v1\"},{\"k2\":\"v2\"}\"]")
	assert.Equal(rp.Pack(jsonArray), ErrInvalidJSONInput)
}

func TestBufferedDataRemainsUnchangedOnPackErrors(t *testing.T) {
	assert := assert.New(t)
	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})

	r := mux.NewRouter()
	r.HandleFunc("/", fastHandler).Methods("POST")
	httptest.NewServer(r)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		BufferSize:        len(data) + 2,
	})
	assert.NoError(err)

	assert.NoError(rp.Pack(data))
	assert.Equal(len(rp.bufferedData), len(data)+1)

	invalidData := []byte("Some string")
	assert.Error(rp.Pack(invalidData))
	assert.Equal(len(rp.bufferedData), len(data)+1)
	assert.Equal(rp.fileNumber, 0)
	assert.Equal(len(rp.fileLocations), 0)

	// Since we'll be packing data larger than the buffer size, this will trigger
	// a write. And since this write will fail the pack will fail and the data
	// should remain unchanged.
	writeToS3 = writeToS3Fail
	defer func() {
		writeToS3 = writeToS3Success
	}()
	assert.Error(rp.Pack(data))
	assert.Equal(len(rp.bufferedData), len(data)+1)
	assert.Equal(rp.fileNumber, 0)
	assert.Equal(len(rp.fileLocations), 0)
}

func TestNoWritesAfterSeal(t *testing.T) {
	assert := assert.New(t)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		JobEndpoint:       testEndpoint,
	})
	assert.NoError(err)

	assert.NoError(rp.Seal())
	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.Equal(rp.Pack(data), ErrPipeIsSealed)

	// Ensure we can still write once unsealed
	assert.NoError(rp.Unseal())
	assert.NoError(rp.Pack(data))
}

func TestUnsuccessfulCustomManifestCreationWhenUnsealed(t *testing.T) {
	assert := assert.New(t)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		JobEndpoint:       testEndpoint,
	})
	assert.NoError(err)

	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(rp.Pack(data))

	manifestName := "test-manifest"
	assert.Equal(rp.CreateAndUploadCustomManifest(manifestName), ErrPackageNotSealed)
	assert.NoError(rp.Seal())
	assert.NoError(rp.CreateAndUploadCustomManifest(manifestName))
}

func TestSuccessfulSend(t *testing.T) {
	assert := assert.New(t)

	r := mux.NewRouter()
	r.HandleFunc("/", fastHandler).Methods("POST")
	server := httptest.NewServer(r)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		JobEndpoint:       server.URL,
	})
	assert.NoError(err)

	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(rp.Pack(data))
	assert.NoError(rp.Send())
}

func TestSuccessfulMultipleSends(t *testing.T) {
	assert := assert.New(t)

	r := mux.NewRouter()
	r.HandleFunc("/", fastHandler).Methods("POST")
	server := httptest.NewServer(r)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		JobEndpoint:       server.URL,
	})
	assert.NoError(err)

	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(rp.Pack(data))
	assert.NoError(rp.Send())

	assert.NoError(rp.Pack(data))
	assert.NoError(rp.Send())
}

func TestSuccessfulSendWhenSealed(t *testing.T) {
	assert := assert.New(t)

	r := mux.NewRouter()
	r.HandleFunc("/", fastHandler).Methods("POST")
	server := httptest.NewServer(r)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		JobEndpoint:       server.URL,
	})
	assert.NoError(err)

	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(rp.Pack(data))
	assert.NoError(rp.Seal())
	assert.NoError(rp.Send())
}

func TestUnsuccessfulSendWithoutEndpoint(t *testing.T) {
	assert := assert.New(t)

	r := mux.NewRouter()
	r.HandleFunc("/", fastHandler).Methods("POST")
	httptest.NewServer(r)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
	})
	assert.NoError(err)

	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(rp.Pack(data))
	assert.Equal(rp.Send(), ErrNoJobEndpoint)
}

func TestUnsuccessfulSendWithInvalidEndpoint(t *testing.T) {
	assert := assert.New(t)

	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		JobEndpoint:       "some_invalid_endpoint",
	})
	assert.NoError(err)

	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(rp.Pack(data))
	assert.Error(rp.Send())
	assert.True(rp.isSealed) // It's important the pipe remains sealed after a failed job post.
}

func TestUnsuccessfulSendWithoutValidConfig(t *testing.T) {
	assert := assert.New(t)

	r := mux.NewRouter()
	r.HandleFunc("/", fastHandler).Methods("POST")
	server := httptest.NewServer(r)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &partialConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		JobEndpoint:       server.URL,
	})
	assert.NoError(err)

	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(rp.Pack(data))
	assert.Error(rp.Send())
	assert.True(rp.isSealed) // It's important the pipe remains sealed after failing with an invalid config.
}

func TestNoOperationsWhenClosed(t *testing.T) {
	assert := assert.New(t)

	r := mux.NewRouter()
	r.HandleFunc("/", fastHandler).Methods("POST")
	server := httptest.NewServer(r)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		JobEndpoint:       server.URL,
	})
	assert.NoError(err)

	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(rp.Pack(data))

	assert.NoError(rp.Close())
	assert.Equal(rp.Pack(data), ErrPipeIsClosed)
	assert.Equal(rp.Seal(), ErrPipeIsClosed)
	assert.Equal(rp.Unseal(), ErrPipeIsClosed)
	assert.Equal(rp.Send(), ErrPipeIsClosed)
	assert.Equal(rp.Reset(), ErrPipeIsClosed)
	assert.NoError(rp.Close())
	assert.NoError(rp.CloseWithoutSending())
}

func TestOperationsErrorDuringSend(t *testing.T) {
	assert := assert.New(t)

	r := mux.NewRouter()
	r.HandleFunc("/", slowHandler).Methods("POST") // Slow handler ensure we have time to test other actions while sending is in progress
	server := httptest.NewServer(r)
	rp, err := NewRedshiftIO(NewRedshiftIOOptions{
		DestinationConfig: &completeConfig,
		S3Bucket:          s3Bucket,
		AWSKey:            awsKey,
		AWSPassword:       awsPassword,
		JobEndpoint:       server.URL,
	})
	assert.NoError(err)

	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(rp.Pack(data))

	// Asyncronously call a Send and ensure any operation fails in the meantime
	sendWg := &sync.WaitGroup{}
	sendWg.Add(1)
	go func() {
		defer sendWg.Done()
		assert.NoError(rp.Send())
	}()

	for !rp.SendingInProgress { // Block until sending is labelled as in progress
	}
	assert.Equal(rp.Pack(data), ErrSendingInProgress)
	assert.Equal(rp.Seal(), ErrSendingInProgress)
	assert.Equal(rp.Unseal(), ErrSendingInProgress)
	assert.Equal(rp.Send(), ErrSendingInProgress)
	assert.Equal(rp.Close(), ErrSendingInProgress)
	assert.Equal(rp.CloseWithoutSending(), ErrSendingInProgress)
	assert.Equal(rp.Reset(), ErrSendingInProgress)
	sendWg.Wait()

	// After sending is finished, ensure operations work as normal
	assert.False(rp.SendingInProgress)
	assert.NoError(rp.Pack(data))
	assert.NoError(rp.Seal())
	assert.NoError(rp.Unseal())
	assert.NoError(rp.Pack(data))
	assert.NoError(rp.Send())
	assert.NoError(rp.Close())
}
