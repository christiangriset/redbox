package s3box

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	// DefaultBufferSize is set to 10MB
	DefaultBufferSize = 10000000
)

var (
	// ErrS3BucketRequired signals an s3 bucket wasn't provided
	ErrS3BucketRequired = fmt.Errorf("An s3 bucket is required to create an s3box.")

	// ErrBoxIsSealed signals an operation which can't occur when a box is sealed.
	ErrBoxIsSealed = fmt.Errorf("Cannot perform action when box is sealed.")

	// ErrInvalidJSONInput captures when the input data can't be marshalled into JSON.
	ErrInvalidJSONInput = fmt.Errorf("Only JSON-able inputs are supported for syncing to Redshift.")
)

// S3Box manages piping data into Redshift. The core idea is to buffer data locally, ship to s3 when too much is buffered, and finally box to Redshift.
type S3Box struct {
	// Inheret mutex locking/unlocking
	sync.Mutex

	// s3Bucket specifies the intermediary bucket before ultimately piping to Redshift. The user should have access to this bucket.
	s3Bucket string

	// s3Handler manages the piping of data to s3
	s3Handler *s3.S3

	// bufferSize is the maximum size of data we're willing to buffer before creating an s3 file
	bufferSize int

	// bufferedData is the data currently buffered in the box. Calling Dump ships this data into s3
	bufferedData []byte

	// timestamp tracks the time a box was created or reset
	timestamp time.Time

	// fileNumber indicates the number of s3 files which have currently been created
	fileNumber int

	// fileLocations stores the s3 files already created
	fileLocations []string

	// isSealed indicates whether writes are currently allows to the buffer
	isSealed bool
}

// NewS3BoxOptions is the expected input for creating a new S3Box
type NewS3BoxOptions struct {
	// S3Bucket specifies the intermediary bucket before ultimately piping to Redshift. The user should have access to this bucket
	S3Bucket string

	// AWSKey is the AWS ACCESS KEY ID
	AWSKey string

	// AWSPassword is the AWS SECRET ACCESS KEY
	AWSPassword string

	// AWSToken is the AWS SESSION TOKEN
	AWSToken string

	// BufferSize is the maximum size of data we're willing to buffer before creating an s3 file
	BufferSize int
}

// NewS3Box creates a new S3Box given the input options, but without the requirement of a destination config.
// Errors occur if there's an invalid input or if there's difficulty setting up an s3 connection.
func NewS3Box(options NewS3BoxOptions) (*S3Box, error) {
	// Check for required inputs and a valid destination config
	if options.S3Bucket == "" {
		return nil, ErrS3BucketRequired
	}

	bufferSize := DefaultBufferSize
	if options.BufferSize > 0 {
		bufferSize = options.BufferSize
	}

	// Setup s3 handler and aws configuration. If no creds are explicitly provided, they'll be grabbed from the environment.
	region, err := getRegionForBucket(options.S3Bucket)
	if err != nil {
		return nil, fmt.Errorf("Failed to get AWS region for bucket %s: (%s)", options.S3Bucket, err)
	}

	// If AWS creds were provided use those, otherwise grab them from your environment
	var awsCreds *credentials.Credentials
	if options.AWSKey == "" && options.AWSPassword == "" && options.AWSToken == "" {
		awsCreds = credentials.NewEnvCredentials()
	} else {
		awsCreds = credentials.NewStaticCredentials(options.AWSKey, options.AWSPassword, options.AWSToken)
	}
	awsConfig := aws.NewConfig().WithRegion(region).WithS3ForcePathStyle(true).WithCredentials(awsCreds)
	awsSession := session.New()

	return &S3Box{
		s3Bucket:   options.S3Bucket,
		timestamp:  time.Now(),
		s3Handler:  s3.New(awsSession, awsConfig),
		bufferSize: bufferSize,
	}, nil
}

// NextBox gives you a new box, forgetting everything about previously packaged data
func (sb *S3Box) NextBox() {
	sb.Lock()
	sb.timestamp = time.Now()
	sb.fileNumber = 0
	sb.fileLocations = []string{}
	sb.bufferedData = []byte{}
	sb.isSealed = false
	sb.Unlock()
}

// Pack writes bytes into a buffer. Once that buffer hits capacity, the data is output to s3.
// Any error will leave the buffer unmodified.
func (sb *S3Box) Pack(data []byte) error {
	if sb.isSealed {
		return ErrBoxIsSealed
	}

	// If the bytes aren't in JSON format, return an error
	var tempMap map[string]interface{}
	if err := json.Unmarshal(data, &tempMap); err != nil {
		return ErrInvalidJSONInput
	}

	sb.Lock()
	oldBuffer := sb.bufferedData // If write fails, keep buffered data unchanged
	data = append(data, '\n')    // Append a new line for text-editor readability
	sb.bufferedData = append(sb.bufferedData, data...)
	sb.Unlock()

	// If we're hitting capacity, dump the results to s3.
	// If shipping to s3 errors, don't modify the buffer.
	if len(sb.bufferedData) > sb.bufferSize {
		if err := sb.dumpToS3(); err != nil {
			sb.Lock()
			sb.bufferedData = oldBuffer
			sb.Unlock()
			return err
		}
	}

	return nil
}

// Seal closes writes and flushes any buffered data to s3. A manifest is then
// created and uploaded to s3 with the given name.
func (sb *S3Box) Seal(manifestName string) error {
	if err := sb.dumpToS3(); err != nil {
		return err
	}

	if err := sb.createAndUploadManifest(manifestName); err != nil {
		return err
	}

	sb.isSealed = true
	return nil
}

// dumpToS3 ships buffered  data to s3 and increments the index with a clean slate of running data
func (sb *S3Box) dumpToS3() error {
	if len(sb.bufferedData) == 0 {
		return nil
	}
	fileKey := fmt.Sprintf("%d_%d.json.gz", sb.timestamp.UnixNano(), sb.fileNumber)
	sb.Lock()
	defer sb.Unlock()
	if err := writeToS3(sb.s3Handler, sb.s3Bucket, fileKey, sb.bufferedData); err != nil {
		return err
	}
	sb.fileNumber++
	sb.bufferedData = []byte{}
	fileName := fmt.Sprintf("s3://%s/%s", sb.s3Bucket, fileKey)
	sb.fileLocations = append(sb.fileLocations, fileName)
	return nil
}

func (sb *S3Box) createAndUploadManifest(manifestName string) error {
	type entry struct {
		URL       string `json:"url"`
		Mandatory bool   `json:"mandatory"`
	}
	type entries struct {
		Entries []entry `json:"entries"`
	}

	var manifest entries
	for _, fileName := range sb.fileLocations {
		manifest.Entries = append(manifest.Entries, entry{
			URL:       fileName,
			Mandatory: true,
		})
	}

	manifestBytes, _ := json.Marshal(manifest)
	log.Printf("Writing manifest to s3://%s/%s\n", sb.s3Bucket, manifestName)
	return writeToS3(sb.s3Handler, sb.s3Bucket, manifestName, manifestBytes)
}
