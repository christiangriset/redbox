package s3box

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
)

const (
	s3Bucket    = "test-bucket"
	awsKey      = "Key"
	awsPassword = "Pass"
	s3Region    = "us-west-1"
)

func getRegionForBucketSuccess(bucket string) (string, error) {
	return s3Region, nil
}

func getRegionForBucketFail(bucket string) (string, error) {
	return "", fmt.Errorf("failed getting bucket location")
}

func writeToS3Success(s3Handler *s3.S3, schema, table string, input []byte, gzip bool) error {
	return nil
}

func writeToS3Fail(s3Handler *s3.S3, schema, table string, input []byte, gzip bool) error {
	return fmt.Errorf("failed writing to s3")
}

func TestMain(m *testing.M) {
	// Assume successful s3 calls by default
	GetRegionForBucket = getRegionForBucketSuccess
	writeToS3 = writeToS3Success

	os.Exit(m.Run())
}

func TestSuccessfulBoxCreation(t *testing.T) {
	assert := assert.New(t)
	// We should be able to successfully create a box with both complete and incomplete configurations.
	_, err := NewS3Box(Options{
		S3Bucket:    s3Bucket,
		AWSKey:      awsKey,
		AWSPassword: awsPassword,
	})
	assert.NoError(err)
}

func TestDontAttemptToGetRegionIfProvided(t *testing.T) {
	// We shouldn't error in creating an S3Box if getting the region fails.
	GetRegionForBucket = getRegionForBucketFail
	defer func() {
		GetRegionForBucket = getRegionForBucketSuccess
	}()

	assert := assert.New(t)
	// We should be able to successfully create a box with both complete and incomplete configurations.
	_, err := NewS3Box(Options{
		S3Bucket:    s3Bucket,
		S3Region:    s3Region,
		AWSKey:      awsKey,
		AWSPassword: awsPassword,
	})
	assert.NoError(err)
}

func TestUnsuccessfulBoxCreation(t *testing.T) {
	assert := assert.New(t)

	// Error if we include a config without either a schema or table
	_, err := NewS3Box(Options{})
	assert.Equal(err, errS3BucketRequired)
}

func TestValidPacks(t *testing.T) {
	assert := assert.New(t)
	sb, err := NewS3Box(Options{
		S3Bucket:    s3Bucket,
		AWSKey:      awsKey,
		AWSPassword: awsPassword,
	})
	assert.NoError(err)

	data1, _ := json.Marshal(map[string]interface{}{"Table": "row"})
	assert.NoError(sb.Pack(data1))
	assert.Equal(len(sb.bufferedData), len(data1)+1) // Account for the appended new line character

	sb, err = NewS3Box(Options{
		S3Bucket:    s3Bucket,
		AWSKey:      awsKey,
		AWSPassword: awsPassword,
	})
	assert.NoError(err)

	data2, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(sb.Pack(data2))
	assert.Equal(len(sb.bufferedData), len(data2)+1) // Account for the appended new line character
}

func TestCorrectNumberOfS3Writes(t *testing.T) {
	assert := assert.New(t)
	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	sb, err := NewS3Box(Options{
		S3Bucket:    s3Bucket,
		AWSKey:      awsKey,
		AWSPassword: awsPassword,
		BufferSize:  len(data), // This is chosen such that each pack will overflow the buffer and "write" to s3
	})
	assert.NoError(err)

	nFiles := 10
	for i := 0; i < nFiles; i++ {
		assert.NoError(sb.Pack(data))
	}
	assert.Equal(len(sb.fileLocations), nFiles)
}

func TestBufferedDataRemainsUnchangedOnPackErrors(t *testing.T) {
	assert := assert.New(t)
	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})

	// r := mux.NewRouter()
	// r.HandleFunc("/", fastHandler).Methods("POST")
	// httptest.NewServer(r)
	sb, err := NewS3Box(Options{
		S3Bucket:    s3Bucket,
		AWSKey:      awsKey,
		AWSPassword: awsPassword,
		BufferSize:  len(data) + 2,
	})
	assert.NoError(err)

	assert.NoError(sb.Pack(data))
	assert.Equal(len(sb.bufferedData), len(data)+1)

	// Since we'll be packing data larger than the buffer size, this will trigger
	// a write. And since this write will fail the pack will fail and the data
	// should remain unchanged.
	writeToS3 = writeToS3Fail
	defer func() {
		writeToS3 = writeToS3Success
	}()
	assert.Error(sb.Pack(data))
	assert.Equal(len(sb.bufferedData), len(data)+1)
	assert.Equal(len(sb.fileLocations), 0)
}

func TestNoWritesAfterManifestCreation(t *testing.T) {
	assert := assert.New(t)
	sb, err := NewS3Box(Options{
		S3Bucket:    s3Bucket,
		AWSKey:      awsKey,
		AWSPassword: awsPassword,
	})
	assert.NoError(err)

	data, _ := json.Marshal(map[string]interface{}{"time": time.Now(), "id": "1234"})
	assert.NoError(sb.Pack(data))
	_, err = sb.CreateManifests("test", 1)
	assert.NoError(err)
	assert.Equal(sb.Pack(data), errBoxIsShipped)
}

func TestCreatesCorrectNumberOfManifests(t *testing.T) {
	assert := assert.New(t)
	sb, err := NewS3Box(Options{
		S3Bucket:    s3Bucket,
		AWSKey:      awsKey,
		AWSPassword: awsPassword,
	})
	assert.NoError(err)

	// Artificially add some file locations
	fileSlug := "test_files"
	nFiles := 10
	for i := 0; i < nFiles; i++ {
		file := fmt.Sprintf("%s_%d.json.gz", fileSlug, i)
		sb.fileLocations = append(sb.fileLocations, file)
	}

	manifestKey := "test"
	nManifests := 5
	manifestLocations, err := sb.CreateManifests(manifestKey, nManifests)
	assert.NoError(err)
	assert.Equal(nManifests, len(manifestLocations))

	// If the number of manifests is greater than the number of files,
	// return only that number of manifests.
	sb.isShipped = false // Hack to override erroring if the box has already shipped
	nManifests = 100
	manifestLocations, err = sb.CreateManifests(manifestKey, nManifests)
	assert.NoError(err)
	assert.Equal(nFiles, len(manifestLocations))
}
