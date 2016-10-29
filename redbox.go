package redbox

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/cgclever/redbox/s3box"
)

var (
	// ErrSendingInProgress captures operations when a send is in progress.
	ErrSendingInProgress = fmt.Errorf("Cannot perform any action when sending is in progress.")

	// ErrIncompleteArgs captures when not enough arguments are given for generating a new Redbox
	ErrIncompleteArgs = fmt.Errorf("Creating a redshift box requires a schema, table and an s3 bucket.")

	// ErrInvalidJSONInput captures when the input data can't be marshalled into JSON.
	ErrInvalidJSONInput = fmt.Errorf("Only JSON-able inputs are supported for syncing to Redshift.")
)

// Redbox manages piping data into Redshift. The core idea is to buffer data locally, ship to s3 when too much is buffered, and finally box to Redshift.
type Redbox struct {
	// Inheret mutex locking/unlocking
	sync.Mutex

	// schema is the schema of the destination
	schema string

	// table is the table name of the destination
	table string

	// awsKey is the AWS ACCESS KEY ID to the s3bucket
	awsKey string

	// awsPassword is the AWS SECRET ACCESS KEY to the s3Bucket
	awsPassword string

	// s3Box manages the transport of data to Redshift.
	s3Box *s3box.S3Box

	// redshift is the direct redshift connection
	redshift Redshift

	// SendingInProgress indicates if a send is in progress
	SendingInProgress bool

	// truncate indicates if we should truncate the destination table
	truncate bool

	// options remembers the options used to configure the instance
	options NewRedboxOptions
}

// NewRedboxOptions is the expected input for creating a new Redbox
type NewRedboxOptions struct {
	// Schema is the destination Redshift table schema
	Schema string

	// Table is the destination Redshift table name
	Table string

	// S3Bucket specifies the intermediary bucket before ultimately piping to Redshift. The user should have access to this bucket.
	S3Bucket string

	// AWSKey is the AWS ACCESS KEY ID
	AWSKey string

	// AWSPassword is the AWS SECRET ACCESS KEY
	AWSPassword string

	// BufferSize is the maximum size of data we're willing to buffer before creating an s3 file
	BufferSize int

	// Truncate indicates if we should truncate the destination table
	Truncate bool

	// RedshiftConfiguration specifies the destination Redshift configuration
	RedshiftConfiguration RedshiftConfiguration
}

// NewRedbox creates a new Redbox given the input options, but without the requirement of a destination config.
// Errors occur if there's an invalid input or if there's difficulty setting up either an s3 or redshift connection.
func NewRedbox(options NewRedboxOptions) (*Redbox, error) {
	if options.Schema == "" || options.Table == "" || options.S3Bucket == "" {
		return nil, ErrIncompleteArgs
	}

	awsKey := options.AWSKey
	if awsKey == "" {
		awsKey = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	awsPassword := options.AWSPassword
	if awsPassword == "" {
		awsPassword = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	s3Box, err := s3box.NewS3Box(s3box.NewS3BoxOptions{
		S3Bucket:    options.S3Bucket,
		AWSKey:      awsKey,
		AWSPassword: awsPassword,
		BufferSize:  options.BufferSize,
	})

	if err != nil {
		return nil, err
	}

	redshift, err := options.RedshiftConfiguration.RedshiftConnection()
	if err != nil {
		return nil, err
	}

	return &Redbox{
		schema:      options.Schema,
		table:       options.Table,
		awsKey:      awsKey,
		awsPassword: awsPassword,
		s3Box:       s3Box,
		redshift:    redshift,
		truncate:    options.Truncate,
		options:     options,
	}, nil
}

// Pack writes a single row of bytes. Currently only configured to accept JSON inputs,
// but will support CSV inputs in the future.
func (rb *Redbox) Pack(row []byte) error {
	var tempMap map[string]interface{}
	if err := json.Unmarshal(row, &tempMap); err != nil {
		return ErrInvalidJSONInput
	}
	return rb.s3Box.Pack(row)
}

// Send ships written data to the destination Redshift table.
func (rb *Redbox) Send() error {
	if rb.SendingInProgress {
		return ErrSendingInProgress
	}

	// Kick off the s3-to-Redshift job
	rb.SendingInProgress = true
	// To be filled in
	rb.SendingInProgress = false

	return nil
}
