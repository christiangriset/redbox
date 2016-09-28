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
	DefaultBufferSize = 10 * 1000 * 1000
)

var (
	// ErrS3BucketRequired signals an s3 bucket wasn't provided
	ErrS3BucketRequired = fmt.Errorf("An s3 bucket is required to create an s3box.")

	// ErrBoxIsSealed signals an operation which can't occur when a box is sealed.
	ErrBoxIsSealed = fmt.Errorf("Cannot perform action when box is sealed.")

	// ErrBoxNotSealed signals an operation which can't occur when a box is not sealed.
	ErrBoxNotSealed = fmt.Errorf("Cannot perform action when box is not sealed.")
)

// S3Box manages piping data into S3. The mechanics are to buffer data locally, ship to s3 when too much is buffered, and finally create manifests pointing to the data files.
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

	// fileLocations stores the s3 files already created
	fileLocations []string

	// isSealed indicates whether writes are currently allows to the buffer
	isSealed bool
}

// NewS3BoxOptions is the expected input for creating a new S3Box.
// Currently only an S3Bucket is required. If AWS vars aren't explicitly provided, they'll
// be pulled from your environment.
type NewS3BoxOptions struct {
	// S3Bucket is the destination s3 bucket.
	// This is required.
	S3Bucket string

	// AWSKey is the AWS ACCESS KEY ID.
	// By default grabs from your environment.
	AWSKey string

	// AWSPassword is the AWS SECRET ACCESS KEY.
	// By default grabs from your environment.
	AWSPassword string

	// AWSToken is the AWS SESSION TOKEN.
	// By default grabs from your environment.
	AWSToken string

	// BufferSize is the maximum size of data we buffer internally
	// before creating an s3 file.
	// This is optional and defaults to 10MB.
	BufferSize int
}

// NewS3Box creates a new S3Box given the input options.
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

// Pack writes bytes into a buffer. Once that buffer hits capacity, the data is output to s3.
// Any error will leave the buffer unmodified.
func (sb *S3Box) Pack(data []byte) error {
	if sb.isSealed {
		return ErrBoxIsSealed
	}

	sb.Lock()
	defer sb.Unlock()
	oldBuffer := sb.bufferedData // If write fails, keep buffered data unchanged
	data = append(data, '\n')    // Append a new line for text-editor readability
	sb.bufferedData = append(sb.bufferedData, data...)

	// If we're hitting capacity, dump the results to s3.
	// If shipping to s3 errors, don't modify the buffer.
	if len(sb.bufferedData) > sb.bufferSize {
		if err := sb.dumpToS3(); err != nil {
			sb.bufferedData = oldBuffer
			return err
		}
	}

	return nil
}

// Seal closes writes and flushes any buffered data to s3.
// Once the box is sealed, you can no longer write to this box.
// Use NextBox to create a new box if you'd like to continue packing.
func (sb *S3Box) Seal() error {
	sb.Lock()
	defer sb.Unlock()
	if err := sb.dumpToS3(); err != nil {
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
	fileNumber := len(sb.fileLocations)
	fileKey := fmt.Sprintf("%d_%d.json.gz", sb.timestamp.UnixNano(), fileNumber)
	if err := writeToS3(sb.s3Handler, sb.s3Bucket, fileKey, sb.bufferedData, true); err != nil {
		return err
	}
	sb.bufferedData = []byte{}
	fileName := fmt.Sprintf("s3://%s/%s", sb.s3Bucket, fileKey)
	sb.fileLocations = append(sb.fileLocations, fileName)
	return nil
}

// CreateManifests takes in a manifest key and splits the s3 files across the
// input number of manifests. If nManifests is greater than the number of generated
// s3 files, you'll only receive manifests back point
func (sb *S3Box) CreateManifests(manifestSlug string, nManifests int) ([]string, error) {
	if !sb.isSealed {
		return nil, ErrBoxNotSealed
	}
	type entry struct {
		URL       string `json:"url"`
		Mandatory bool   `json:"mandatory"`
	}
	type entries struct {
		Entries []entry `json:"entries"`
	}

	if nManifests > len(sb.fileLocations) {
		nManifests = len(sb.fileLocations)
	}
	manifests := make([]entries, nManifests)

	// Evenly distribute the file locations across the manifests
	for i, fileName := range sb.fileLocations {
		index := i % nManifests
		manifests[index].Entries = append(manifests[index].Entries, entry{
			URL:       fileName,
			Mandatory: true,
		})
	}

	manifestLocations := make([]string, nManifests)
	for i, manifest := range manifests {
		manifestBytes, _ := json.Marshal(manifest)
		manifestName := fmt.Sprintf("%s_%d.manifest", manifestSlug, i)
		manifestLocations[i] = manifestName
		if err := writeToS3(sb.s3Handler, sb.s3Bucket, manifestName, manifestBytes, false); err != nil {
			return nil, err
		}
		log.Printf("Wrote manifest to s3://%s/%s\n", sb.s3Bucket, manifestName)
	}

	return manifestLocations, nil
}