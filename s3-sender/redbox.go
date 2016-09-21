package redbox

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
	// ErrIncompleteDestinationConfig indicates that either a schema or table name are missing
	ErrIncompleteDestinationConfig = fmt.Errorf("Destination config input must include a schema and table name.")

	// ErrSendingInProgress captures operations when a send is in progress.
	ErrSendingInProgress = fmt.Errorf("Cannot perform any action when sending is in progress.")

	// ErrIncompleteArgs captures when not enough arguments are given for generating a new Redbox
	ErrIncompleteArgs = fmt.Errorf("Creating a redshift box requires a distination config and an s3 bucket.")

	// ErrBoxIsSealed signals an operation which can't occur when a box is sealed.
	ErrBoxIsSealed = fmt.Errorf("Cannot perform action when box is sealed.")

	// ErrNoJobEndpoint indicates we can't send without a job endpoint
	ErrNoJobEndpoint = fmt.Errorf("Cannot send s3-to-Redshift job without an endpoint.")

	// ErrInvalidConfig indicates the config is invalid for sending
	ErrInvalidConfig = fmt.Errorf("Cannot perform send with an invalid configuration. It's recommended you create a test to ensure valid configuration inputs.")

	// ErrInvalidJSONInput captures when the input data can't be marshalled into JSON.
	ErrInvalidJSONInput = fmt.Errorf("Only JSON-able inputs are supported for syncing to Redshift.")

	// ErrBoxNotSealed captures trying to create a custom manifest on an unsealed stream
	ErrBoxNotSealed = fmt.Errorf("Can only create a custom manifest on a sealed stream.")

	// ErrNotEnoughInstructionsToSendJob captures when not
	ErrNotEnoughInstructionsToSendJob = fmt.Errorf("To send a job, the user must include an s3-to-Redshift configuration.")
)

// Redbox manages piping data into Redshift. The core idea is to buffer data locally, ship to s3 when too much is buffered, and finally box to Redshift.
type Redbox struct {
	// Inheret mutex locking/unlocking
	sync.Mutex

	// schema is the schema of the destination
	schema string

	// table is the table name of the destination
	table string

	// s3Bucket specifies the intermediary bucket before ultimately piping to Redshift. The user should have access to this bucket.
	s3Bucket string

	// s3Handler manages the piping of data to s3
	s3Handler *s3.S3

	// jobEndpoint is the endpoint responsible for kicking off an s3-to-Redshift job
	jobEndpoint string

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

	// SendingInProgress indicates if a send is in progress
	SendingInProgress bool

	// truncate indicates if we should truncate the destination table
	truncate bool

	// force indicates whether we should force the data into redshift without the protection of checking for data duplication
	force bool

	// s3ToRedshift stores a provided s3-to-Redshift configuration
	s3ToRedshift *S3ToRedshift
}

// NewRedboxOptions is the expected input for creating a new Redbox
type NewRedboxOptions struct {
	// Schema describes the destination table of the data.
	Schema string

	// Table is the destination table name
	Table string

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

	// S3ToRedshiftConfig configures the redbox to use an s3-to-Redshift hookup
	S3ToRedshiftConfig *S3ToRedshiftConfig

	DirectRedshiftConfig

	// Truncate indicates if we should truncate the destination table
	Truncate bool

	// DisableDataProtection indicates whether we should force the data into redshift
	// without the protection of checking for data duplication.
	DisableDataProtection bool

	// Granularity tells how far apart data timestamps need to be
	// when data protection is enabled. Defaults to one hour.
	Granularity time.Duration
}

// NewRedbox creates a new Redbox given the input options, but without the requirement of a destination config.
// Errors occur if there's an invalid input or if there's difficulty setting up an s3 connection.
func NewRedbox(options NewRedboxOptions) (*Redbox, error) {
	// Check for required inputs and a valid destination config
	if options.Schema == "" || options.Table == "" || options.S3Bucket == "" {
		return nil, ErrIncompleteArgs
	}

	bufferSize := DefaultBufferSize
	if options.BufferSize > 0 {
		bufferSize = options.BufferSize
	}

	granularity := time.Hour
	if options.Granularity != 0 {
		granularity = options.Granularity
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
	s3Handler := s3.New(awsSession, awsConfig)

	rb := &Redbox{
		schema:     options.Schema,
		table:      options.Table,
		s3Bucket:   options.S3Bucket,
		timestamp:  time.Now(),
		s3Handler:  s3Handler,
		bufferSize: bufferSize,
		truncate:   options.Truncate,
		force:      options.DisableDataProtection,
	}

	// If an s3-to-Redshift config was provided fill it in and add to the Redbox.
	if options.S3ToRedshiftConfig != nil {
		conf := options.S3ToRedshiftConfig

		s3ToRedshift := &S3ToRedshift{
			JobEndpoint: conf.JobEndpoint,
			S3Bucket:    options.S3Bucket,
			S3Handler:   s3Handler,
			Schema:      options.Schema,
			Table:       options.Table,
			Columns:     conf.Columns,
			Granularity: granularity,
			Truncate:    options.Truncate,
			Force:       options.DisableDataProtection,
		}

		if err := s3ToRedshift.Validate(); err != nil {
			return nil, err
		}
		rb.s3ToRedshift = s3ToRedshift

	}

	return rb, nil
}

// NextBox gives you a new box, forgetting everything about previously packaged data
func (rp *Redbox) NextBox() error {
	if rp.SendingInProgress {
		return ErrSendingInProgress
	}
	rp.Lock()
	rp.timestamp = time.Now()
	rp.fileNumber = 0
	rp.fileLocations = []string{}
	rp.bufferedData = []byte{}
	rp.isSealed = false
	rp.Unlock()
	return nil
}

// Pack writes bytes into a buffer. Once that buffer hits capacity, the data is output to s3.
// Any error will leave the buffer unmodified.
func (rp *Redbox) Pack(data []byte) error {
	if rp.SendingInProgress {
		return ErrSendingInProgress
	}
	if rp.isSealed {
		return ErrBoxIsSealed
	}

	// If the bytes aren't in JSON format, return an error
	var tempMap map[string]interface{}
	if err := json.Unmarshal(data, &tempMap); err != nil {
		return ErrInvalidJSONInput
	}

	rp.Lock()
	oldBuffer := rp.bufferedData // If write
	data = append(data, '\n')    // Append a new line for text-editor readability
	rp.bufferedData = append(rp.bufferedData, data...)
	rp.Unlock()

	// If we're hitting capacity, dump the results to s3.
	// If shipping to s3 errors, don't modify the buffer.
	if len(rp.bufferedData) > rp.bufferSize {
		if err := rp.dumpToS3(); err != nil {
			rp.Lock()
			rp.bufferedData = oldBuffer
			rp.Unlock()
			return err
		}
	}

	return nil
}

// Seal closes writes and flushes any buffered data to s3. Call Unseal to enable writing again.
func (rp *Redbox) Seal() error {
	if rp.SendingInProgress {
		return ErrSendingInProgress
	}

	if err := rp.dumpToS3(); err != nil {
		return err
	}

	rp.isSealed = true
	return nil
}

// dumpToS3 ships buffered  data to s3 and increments the index with a clean slate of running data
func (rp *Redbox) dumpToS3() error {
	if len(rp.bufferedData) == 0 {
		return nil
	}
	fileKey := fmt.Sprintf("%s_%s_%d_%d.json.gz", rp.schema, rp.table, rp.timestamp.Unix(), rp.fileNumber)
	rp.Lock()
	defer rp.Unlock()
	if err := writeToS3(rp.s3Handler, rp.s3Bucket, fileKey, rp.bufferedData); err != nil {
		return err
	}
	rp.fileNumber++
	rp.bufferedData = []byte{}
	fileName := fmt.Sprintf("s3://%s/%s", rp.s3Bucket, fileKey)
	rp.fileLocations = append(rp.fileLocations, fileName)
	return nil
}

// createAndUploadManifest creates a manifest with a default convention and uploads it to s3.
func (rp *Redbox) createAndUploadManifest() error {
	timestamp := rp.timestamp.Round(time.Hour).UTC().Format(time.RFC3339) //
	defaultManifestName := fmt.Sprintf("%s_%s_%s.manifest", rp.schema, rp.table, timestamp)
	return rp.CreateAndUploadCustomManifest(defaultManifestName)
}

// CreateAndUploadCustomManifest generates a manifest with a custom name, pointing to the location of each data file generated.
//
// NOTE: This function is meant for custom functionality for use outside of s3-to-Redshift.
func (rp *Redbox) CreateAndUploadCustomManifest(manifestName string) error {
	type entry struct {
		URL       string `json:"url"`
		Mandatory bool   `json:"mandatory"`
	}
	type entries struct {
		Entries []entry `json:"entries"`
	}

	if err := rp.Seal(); err != nil {
		return err
	}

	var manifest entries
	for _, fileName := range rp.fileLocations {
		manifest.Entries = append(manifest.Entries, entry{
			URL:       fileName,
			Mandatory: true,
		})
	}

	manifestBytes, _ := json.Marshal(manifest)
	log.Printf("Writing manifest to s3://%s/%s\n", rp.s3Bucket, manifestName)
	return writeToS3(rp.s3Handler, rp.s3Bucket, manifestName, manifestBytes)
}

// Send flushes any data remaining in the buffer and kicks off an s3-to-redshift job which
// ultimately pipes all data to the specified Redshift table.
// Send requires a validated destination config.
// NOTE: An unsuccessful keeps the box closed.
func (rp *Redbox) Send() error {
	if rp.SendingInProgress {
		return ErrSendingInProgress
	}

	// If no data was ever writen, then simply return
	if rp.fileNumber == 0 && len(rp.bufferedData) == 0 {
		return nil
	}

	// Dump any remaining data which hasn't been shipped to s3, prevent writes, and upload the manifest and configs
	if err := rp.createAndUploadManifest(); err != nil {
		return err
	}

	// Kick off the s3-to-Redshift job
	rp.setSendingInProgress(true)
	sendErr := rp.sendToRedshift()
	rp.setSendingInProgress(false)
	if sendErr != nil {
		return sendErr
	}

	rp.NextBox()
	return nil
}

// sendToRedshift is a parent function which routes which function will be responsible
// for piping data to redshift.
func (rp *Redbox) sendToRedshift() error {
	if rp.s3ToRedshift != nil {
		return rp.s3ToRedshift.SendJob(rp.timestamp)
	}
	return ErrNotEnoughInstructionsToSendJob
}

// postS3ToRedshiftJob constructs a payload for an s3-to-Redshift worker
func (rp *Redbox) postS3ToRedshiftJob() error {
	client := &http.Client{}
	payload := fmt.Sprintf("--bucket %s --schema %s --tables %s --date %s --gzip", rp.s3Bucket, rp.schema, rp.table, rp.timestamp)
	if rp.truncate {
		payload += " --truncate"
	}
	if rp.force {
		payload += " --force"
	}
	req, err := http.NewRequest("POST", rp.jobEndpoint, bytes.NewReader([]byte(payload)))
	if err != nil {
		return fmt.Errorf("Error creating new request: %s", err)
	}
	req.Header.Add("Content-Type", "text/plain")
	_, err = client.Do(req)
	if err != nil {
		return fmt.Errorf("Error submitting job:%s", err)
	}
	return nil
}

func (rp *Redbox) setSendingInProgress(sendingInProgress bool) {
	rp.Lock()
	defer rp.Unlock()
	rp.sendingInProgress = sendingInProgress
}

func (rp *Redbox) getSendingInProgress() bool {
	rp.Lock()
	defer rp.Unlock()
	return rp.sendingInProgress
}
