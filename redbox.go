package redbox

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cgclever/redbox/s3box"
)

const defaultNumManifests = 4

var (
	errShippingInProgress = fmt.Errorf("cannot perform any action when shipping is in progress")
	errIncompleteArgs     = fmt.Errorf("creating a redshift box requires a schema, table and an s3 bucket")
	errInvalidJSONInput   = fmt.Errorf("only JSON inputs are supported")
	errBoxShipped         = fmt.Errorf("cannot perform any actions, the box has been shipped")
	errNothingToShip      = fmt.Errorf("cannot perform send, no data was packed")
)

// Redbox manages piping data into Redshift.
// An S3Box is used to manage data transport to S3 via Pack, after
// which Ship commits the data to Redshift.
type Redbox struct {
	// Inherit mutex locking/unlocking
	mt sync.Mutex

	// o holds the options used for configurating the Redbox instance
	o Options

	// s3Box manages the transport of data to Redshift
	s3Box s3box.API

	// redshift is the direct redshift connection
	redshift *sql.DB

	// shippingInProgress indicates if a send is in progress
	shippingInProgress bool

	// shipped indicates if the box has been shipped
	shipped bool
}

// Options specifies the configuration for a new Redbox
type Options struct {
	// Schema is the destination Redshift table schema
	Schema string

	// Table is the destination Redshift table name
	Table string

	// S3Bucket specifies the intermediary bucket before ultimately piping to Redshift.
	// The user must have both read and write access to this bucket.
	S3Bucket string

	// S3Region is the location of the S3Bucket.
	//
	// If not provided Redbox will attempt to locate the region via the AWS API.
	// The user will need to have 'GetBucketLocation' permissions enabled
	// on the target S3 bucket for this feature.
	S3Region string

	// AWSKey is the AWS ACCESS KEY ID
	AWSKey string

	// AWSPassword is the AWS SECRET ACCESS KEY
	AWSPassword string

	// BufferSize is the maximum size of data, in bytes, we're willing to buffer
	// before creating an s3 file.
	BufferSize int

	// NumManifests is an optional parameter choosing how many manifests
	// to break data into. When data transfer gets to several gigabytes
	// the user may need to experiment with larger manifest numbers to prevent
	// timeouts.
	//
	// Note: This number isn't  autocalculated as
	// different cluster configurations can handle different influxes
	// of data. However the number defaults to 4.
	NumManifests int

	// Truncate indicates if we should clear the destination table before
	// transferring data. This is useful for tables representing snapshots
	// of the world.
	Truncate bool

	// RedshiftConfiguration specifies the destination Redshift configuration
	RedshiftConfiguration RedshiftConfiguration
}

// newRedboxInjection returns an Redbox with given input s3Box and redshift inputs.
func newRedboxInjection(options Options, s3Box s3box.API, redshift *sql.DB) *Redbox {
	return &Redbox{
		o:        options,
		s3Box:    s3Box,
		redshift: redshift,
	}
}

// NewRedbox creates a new Redbox given the input options.
// Errors occur if there's an invalid input or if there's
// difficulty setting up either an s3 or redshift connection.
func NewRedbox(options Options) (*Redbox, error) {
	if options.Schema == "" || options.Table == "" || options.S3Bucket == "" {
		return nil, errIncompleteArgs
	}

	if options.AWSKey == "" {
		options.AWSKey = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if options.AWSPassword == "" {
		options.AWSPassword = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}

	if options.S3Region == "" {
		s3Region, err := s3box.GetRegionForBucket(options.S3Bucket)
		if err != nil {
			return nil, err
		}
		options.S3Region = s3Region
	}

	s3Box, err := s3box.NewS3Box(s3box.Options{
		S3Bucket:    options.S3Bucket,
		S3Region:    options.S3Region,
		AWSKey:      options.AWSKey,
		AWSPassword: options.AWSPassword,
		BufferSize:  options.BufferSize,
	})
	if err != nil {
		return nil, err
	}

	redshift, err := options.RedshiftConfiguration.RedshiftConnection()
	if err != nil {
		return nil, err
	}

	if options.NumManifests <= 0 {
		options.NumManifests = defaultNumManifests
	}

	return newRedboxInjection(options, s3Box, redshift), nil
}

// Pack writes a single row of bytes. Currently accepts JSON inputs.
// Pack is concurrency safe.
func (rb *Redbox) Pack(row []byte) error {
	if rb.isShipped() {
		return errBoxShipped
	}
	if rb.isShippingInProgress() {
		return errShippingInProgress
	}

	var tempMap map[string]interface{}
	if err := json.Unmarshal(row, &tempMap); err != nil {
		return errInvalidJSONInput
	}
	return rb.s3Box.Pack(row)
}

// Ship ships written data to the destination Redshift table.
// While shipping is in progress, no other operations are permitted.
// Ship is transactional, meaning that any returned error means
// the destination table has remained unchanged.
func (rb *Redbox) Ship() ([]string, error) {
	if rb.isShipped() {
		return nil, errBoxShipped
	}
	if rb.isShippingInProgress() {
		return nil, errShippingInProgress
	}

	// Kick off the s3-to-Redshift job
	rb.setShippingInProgress(true)
	defer func() {
		rb.setShippingInProgress(false)
	}()

	manifests, err := rb.s3Box.CreateManifests(rb.manifestSlug(), rb.o.NumManifests)
	if err != nil {
		return nil, err
	}
	if len(manifests) == 0 { // If no data was written, there's nothing to ship.
		return nil, errNothingToShip
	}

	if err := rb.copyToRedshift(manifests); err != nil {
		return nil, err
	}

	rb.markShipped()
	return manifests, nil
}

// manifestSlug defines a convention for the slug of each manifest file.
func (rb *Redbox) manifestSlug() string {
	return fmt.Sprintf("%s_%s_%s", rb.o.Schema, rb.o.Table, time.Now().Format(time.RFC3339))
}

// copyToRedshift transports data pointed to by the manifests into Redshift.
// If the truncate flag is present the destination table is first cleared.
func (rb *Redbox) copyToRedshift(manifests []string) error {
	tx, err := rb.redshift.Begin()
	if err != nil {
		return err
	}

	if rb.o.Truncate {
		delStmt := fmt.Sprintf("DELETE FROM \"%s\".\"%s\"", rb.o.Schema, rb.o.Table)
		if _, err := tx.Exec(delStmt); err != nil {
			tx.Rollback()
			return err
		}
	}

	for _, manifest := range manifests {
		copyStmt := rb.copyStatement(manifest)
		if _, err := tx.Exec(copyStmt); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

// copyStatment generates the COPY statement for the given manifest and Redbox configuration
func (rb *Redbox) copyStatement(manifest string) string {
	manifestURL := fmt.Sprintf("s3://%s/%s", rb.o.S3Bucket, manifest)
	copy := fmt.Sprintf("COPY \"%s\".\"%s\" FROM '%s' MANIFEST REGION '%s'", rb.o.Schema, rb.o.Table, manifestURL, rb.o.S3Region)
	dataFormat := "GZIP JSON 'auto'"
	options := "TIMEFORMAT 'auto' TRUNCATECOLUMNS STATUPDATE ON COMPUPDATE ON"
	creds := fmt.Sprintf("CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'", rb.o.AWSKey, rb.o.AWSPassword)
	return fmt.Sprintf("%s %s %s %s", copy, dataFormat, options, creds)
}

func (rb *Redbox) setShippingInProgress(inProgress bool) {
	rb.mt.Lock()
	defer rb.mt.Unlock()
	rb.shippingInProgress = inProgress
}

func (rb *Redbox) markShipped() {
	rb.mt.Lock()
	defer rb.mt.Unlock()
	rb.shipped = true
}

// isShippingInProgress exposes whether a send is in progress.
func (rb *Redbox) isShippingInProgress() bool {
	rb.mt.Lock()
	defer rb.mt.Unlock()
	return rb.shippingInProgress
}

// isShipped exposes whether the box has been shipped
func (rb *Redbox) isShipped() bool {
	rb.mt.Lock()
	defer rb.mt.Unlock()
	return rb.shipped
}
