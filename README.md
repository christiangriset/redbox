

# redbox

Library aiding data transport to Redshift through straighforward configuration and intuitive methods (Pack and Send).

The core functionality is the streaming of (JSON formatted) data into s3 while managing consistent file sizes and easy creation of manifests.
The power of this library comes when pairing with an [s3-to-Redshift](https://github.com/clever/s3-to-redshift) worker. This enables
the "Send" feature automating the kick off of an s3-To-Redshift job.

Even without an s3-to-Redshift hookup, this is a well organized utility for general streaming to s3 and managing manifests for custom COPY commands.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Usage](#usage)
  - [Redbox - The Methods](#redshiftio---the-methods)
    - [Pack(data []bytes) error](#packdata-bytes-error)
    - [Seal() error](#seal-error)
    - [Send() error (Requires s3-to-Redshift hookup)](#send-error-requires-s3-to-redshift-hookup)
    - [CreateAndUploadCustomManifest(manifestKey) error](#createanduploadcustommanifestmanifestkey-error)
    - [NextBox() error](#reset-error)
  - [Redbox - The Configuration](#redshiftio---the-configuration)
    - [DestinationConfig](#destinationconfig)
      - [Validate() error](#validate-error)
    - [NewRedboxOptions](#newredshiftiooptions)
- [Example With s3-to-Redshift Hookup](#example-with-s3-to-redshift-hookup)
- [Example Without s3-to-Redshift Hookup](#example-without-s3-to-redshift-hookup)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->
# Usage

The two primary types supplied are `Redbox` and `DestinationConfig`. Setting up a Redbox requires a DestinationConfig. For exmaple:

```
dc := &DestinationConfig{
  Schema: "schema",
  Table: "table",
  Columns: []Column{
    Column{Name: "time", Type:"timestamp"},
    Column{Name: "id", Type:"text"}
  }
  DataTimestampColumn: "time"
}

r, err := NewRedbox(&NewRedboxOptions{
    DestinationConfig: dc,
    S3Bucket: "bucket-with-user-access",
    AWSKey: yourAWSAccessKeyID,
    AWSPassword: yourAWSSecretAccessKey,
  })
```

## Redbox - The Methods

Redbox is the workhorse manager. Below is an overview of the methods and setup.

### Pack(data []byte) error

Pack buffers the data without sending to Redshift. Once the buffer grows larger than it's maximum size (10MB by default)
the data is gziped and streamed to s3.

Currently Pack is a single row operation which *only* accepts JSONifiable inputs, i.e. those marshalable into a map.

Pack is concurrency safe.

### Seal() error

Seal flushes any buffered data to s3 and prevents further Packing.

### Send() error (Requires s3-to-Redshift hookup)

Send seals the stream, generates the s3 manifest and configuration files and kicks off an s3-to-Redshift job.
While a Send is in progress **all methods** will error. After a successful send the stream loses memory of the previous
data it streams an starts anew. This enables multiple send commands without worrying about duplicating data.

The field `Redbox.SendingInProgress` is exposed to help the user manage other operations during a send.

**Note** An unsuccessful Send will keep the stream sealed.

### CreateAndUploadCustomManifest(manifestKey) error

This is the primary utility for users not intending to utilize an s3-to-Redshift hookup.
Once the package is sealed (and thus all data is flushed to s3), this creates a manifest in s3
with the custom manifest name. This will also seal the box.

The user can then set off their own custom COPY commands utilizing this manifest.

**Note**: The input should *not* be the full s3 path. The configuration will already include the bucket and create the full path for you. This helps prevent cases where a different bucket from the input configuration is supplied.

### NextBox() error

NextBox gives you a new box, forgetting everything about previously packaged data

## Redbox - The Configuration

### DestinationConfig

```
type DestinationConfig struct {
	Schema              string   // Required
	Table               string   // Required
	Columns             []Column // Required for s3-to-Redshift transport
	DataTimestampColumn string   // Required for s3-to-Redshift transport
}

type Column struct {
	Name       string
	Type       string
	SortOrd    int
  	DistKey    bool
	DefaultVal string
	NotNull    bool
	PrimaryKey bool
}
```

The DataTimestampColumn is the column name which indicates the time the data was created. While it's recommended as a general practice to have such a column, it's required for a successful s3-to-Redshift run.

The DistKey is the sharding key (there can be at most one enabled).

SortOrd is the order at which the column should be sorted (in the same way in SQL you'd end with something like `ORDER BY time, id, name,...`).

The supported types for columns are currently (Type -> SQL type):
```
"boolean" -> "boolean"
"float" -> "double precision"
"int" -> "integer"
"timestamp" -> "timestamp without time zone"
"text" -> "character varying(256)"
"longtext" -> "character varying(65535)"
```

#### Validate() error 

DestinationConfig comes with the method `Validate()` which returns an error if the configuration is invalid, e.g. it has multiple dist keys. `Send` will run `Validate()` and therefore will fail if the user supplied an invalid configuration.

### NewRedboxOptions

```
type NewRedboxOptions struct {
	// Required inputs
	DestinationConfig *DestinationConfig
	S3Bucket          string
	AWSKey            string
	AWSPassword       string
	
	// Optional
	AWSToken    string
	BufferSize  int    // Default 10MB
	JobEndpoint string // Endpoint for posting an s3-to-Redshift job. Required for s3-to-Redshift hookup
	Truncate    bool   // Flag to truncate the destination table upon Send
	Force       bool   // Forgos s3-to-Redshift's data protection against duplicate rows
}
```

The expected usecase around controlling BufferSize is for worker memory management. To comfortably use this package at least `2*BufferSize` of memory should be available at any time.

# Example With s3-to-Redshift Hookup

```
type Row struct {
  Time time.Time `json:"time"`
  ID   string    `json:"id"`,
}

func SomeJob() {
  // Setup
  dc := &DestinationConfig{
    Schema: "schema",
    Table: "table",
    Columns: []Column{
      Column{Name: "time", Type:"timestamp", SortOrd:1},
      Column{Name: "id", Type:"text", DistKey:true},
    }
    DataTimestampColumn: "time",
  }

  r, err := NewRedbox(&NewRedboxOptions{
    DestinationConfig: dc,
    S3Bucket: "bucket-with-user-access",
    AWSKey: yourAWSAccessKeyID,
    AWSPassword: yourAWSSecretAccessKey,
    JobEndpoint: pathToWorker,
  })
  handleError(err)
  
  // Data Transfer
  dataStore := getSomeDataStore()
  for dataStore.Iter() {
    rowData := dataStore.GetNextRow() // Return a single Row object
    rowBytes, _ := json.Marshal(rowData)
    handleError(r.Pack(rowBytes))
  }

  handleError(r.Send())
}
```

# Example Without s3-to-Redshift Hookup

```
type Row struct {
  Time time.Time `json:"time"`
  ID   string    `json:"id"`
}

func SomeJob() {
  // Setup
  dc := &DestinationConfig{
    Schema: "schema",
    Table: "table",
  }

  r, err := NewRedbox(&NewRedboxOptions{
    DestinationConfig: dc,
    S3Bucket: "bucket-with-user-access",
    AWSKey: yourAWSAccessKeyID,
    AWSPassword: yourAWSSecretAccessKey,
  })
  handleError(err)
  
  // Data Transfer
  dataStore := getSomeDataStore()
  for dataStore.Iter() {
    rowData := dataStore.GetNextRow() // Return a single Row object
    rowBytes, _ := json.Marshal(rowData)
    handleError(r.Pack(rowBytes))
  }

  manifestKey := "data_locations.manifest"
  handleError(r.CreateAndUploadCustomManifest(manifestKey))
  handleError(runSomeCustomCopyCommand(manifestKey))
  handleError(r.NextBox())
  
  // Process more data and run more COPYs
  ...
}
