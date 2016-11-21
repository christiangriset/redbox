# S3Box

Library aiding data transport to s3 through straighforward configuration and intuitive methods (Packand CreateManifest).

The core under-the-hood functionality is the streaming of data into s3 while managing consistent file sizes and easy creation of manifests.

See both below and the [the godocs](https://godoc.org/github.com/cgclever/redbox/s3box) for API documentation.

## S3Box - The Configuration

### NewS3Box

`func NewS3Box(options Options) (*S3Box, error)`

### Options

```
type NewS3oxOptions struct {
	// Required inputs
	S3Bucket          string

  // Optional AWS creds. If not provided they'll be grabbed from the environment.
	AWSKey            string
	AWSPassword       string
	AWSToken          string
	
  // BufferSize controls the amount of data, in bytes, stored in each s3 file.
  //
  // For memory management, at least `2*BufferSize` of memory should be available
  // at any time. Defaults to 100MB.
	BufferSize  int
}
```

## S3Box - The Methods

### Pack

`func Pack(data []byte) error`

Pack buffers the data. Once the buffer grows larger than the supplied `BufferSize`
the data is gziped and streamed to s3.

Pack is concurrency safe.

### CreateManifests

`func CreateManifests(manifestKey string, numManifests int) ([]string, error)`

This evenly distributes all generated data files into the specified number of manifest files, which are then uploaded to s3.

The user can then set off their own custom COPY commands utilizing these manifests.

**Note1**: The input should *not* be the full s3 path. The configuration will already include the bucket and create the full path for you. This helps prevent cases where a different bucket from the input configuration is supplied.

**Note2**: If the number of generated data files is less than `numManifests`, the return will be a number of manifests equal to the number of data files.

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
  sb, err := s3box.NewS3Box(s3box.Options{
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
