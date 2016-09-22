<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [S3Box](#s3box)
  - [S3Box - The Configuration](#s3box---the-configuration)
    - [NewS3Box](#news3box)
    - [NewS3BoxOptions](#news3boxoptions)
  - [S3Box - The Methods](#s3box---the-methods)
    - [Pack](#pack)
    - [Seal](#seal)
    - [CreateManifests](#createmanifests)
    - [NextBox](#nextbox)
- [Example](#example)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# S3Box

Library aiding data transport to s3 through straighforward configuration and intuitive methods (Pack, Seal and CreateManifest).

The core under-the-hood functionality is the streaming of (JSON formatted) data into s3 while managing consistent file sizes and easy creation of manifests.

See both below and the [the godocs](https://godoc.org/github.com/cgclever/redbox/s3box) for API documentation.

## S3Box - The Configuration

### NewS3Box

`func NewS3Box(options NewS3BoxOptions) (*S3Box, error)`

### NewS3BoxOptions

```
type NewS3oxOptions struct {
	// Required inputs
	S3Bucket          string

  // Optional AWS creds. If not provided they'll be grabbed from the environment.
	AWSKey            string
	AWSPassword       string
	AWSToken          string
	
	// Optional
	BufferSize  int    // Default 10MB
}
```

The expected usecase around controlling BufferSize is for worker memory management. To comfortably use this package at least `2*BufferSize` of memory should be available at any time.

## S3Box - The Methods

### Pack

`func Pack(data []byte) error`

Pack buffers the data. Once the buffer grows larger than it's maximum size (10MB by default)
the data is gziped and streamed to s3.

Currently Pack is a single row operation which *only* accepts JSONifiable inputs, i.e. those marshalable into a map.

Pack is concurrency safe.

### Seal

`func Seal() error`

Seal flushes any buffered data to s3 and prevents further Packing.

### CreateManifests

`func CreateManifests(manifestKey string, numManifests int) ([]string, error)`

Once the package is sealed (and thus all data is flushed to s3), this evenly distributes
all generated data files into a number of manifest files. These manifests are also uploaded
to s3.

The user can then set off their own custom COPY commands utilizing these manifests.

**Note1**: The input should *not* be the full s3 path. The configuration will already include the bucket and create the full path for you. This helps prevent cases where a different bucket from the input configuration is supplied.

**Note2**: If the number of generated data files is less than `numManifests`, the return will be a number of manifests equal to the number of data files. 

### NextBox

`func NextBox()`

NextBox gives you a new box, forgetting everything about previously packaged data.


# Example
```
import (
    "github.com/cgclever/redbox/s3box"
)

type Row struct {
  Time    time.Time `json:"time"`
  Value   string    `json:"value"`
}

func SomeJob() {
  // Setup
  sb, err := s3box.NewS3Box(s3box.NewS3BoxOptions{
    S3Bucket: "bucket-with-user-access",
    AWSKey: yourAWSAccessKeyID,
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
```
