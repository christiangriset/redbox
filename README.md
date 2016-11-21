# Redbox

Library simplifying data transport to Redshift via straighforward configuration and intuitive methods (Pack and Ship).

Redbox is transactional. User packs data until they're finished, after which a call to Ship transports the data to Redshift.

See below and [the Godocs](https://godoc.org/github.com/cgclever/redbox) for API documentation

## Redbox - The Configuration

### RedshiftConfiguration

```
type RedshiftConfiguration struct {
  Host              string
  Port              string
  User              string
  Password          string
  Database          string
  ConnectionTimeout int    // Defaults to 10 seconds
}
```

### Options

```
type Options struct {
  // Required inputs
  Schema                string
  Table                 string
  S3Bucket              string
  RedshiftConfiguration RedshiftConfiguration

  // Truncate clears the destination table before transporting data.
  // This is useful for tables representing snapshots of the world.
  Truncate              bool

  // Optional region of the S3Bucket. If not provided Redbox attempts to use 
  // the AWS API to get its location, however requires the user have permission for this action.
  S3Region string

  // Optional AWS creds. If not provided they'll be grabbed from the environment.
  AWSKey      string
  AWSPassword string
	
  // BufferSize sets the files sizes, in bytes, uploaded to S3. Defaults to 100MB.
  //
  // This is useful for memory management and `2*BufferSize` should be comfortably available.
  // For efficient COPY to Redshift, AWS recommends this lie between 10MB and 1GB.
  BufferSize int

  // NumManifests splits the data across its number of manifest files, performing that
  // number of separate COPY commands. Defaults to 4.
  //
  // For extremely large data transports, Redshift COPYs may timeout with a single manifest.
  // The default should be sufficient for most use cases, otherwise consider increasing.
  NumManifests int
}
```

**Note:** The AWS credentials must have both read and write access to the S3 bucket.

## Redbox - The Methods

### Pack(data []byte) error

Pack buffers data without sending to Redshift and is concurrency safe.

Currently Pack is a single row operation which *only* accepts JSONifiable inputs, i.e. those marshalable into a `map[string]interface{}`.

### Ship() ([]string, error)

Ship commits all packed data to Redshift. If "Truncate" is provided in the configuration, the destination table will first be deleted.
The return is a list of manifests pointing to each data file generated, see [the AWS documentation](http://docs.aws.amazon.com/redshift/latest/dg/loading-data-files-using-manifest.html).
Ship is transactional, meaning any returned error implies the destination table has been left unchanged.

## Example

```
func SomeJob() {
  // Setup. AWS creds determined from environment.
  redbox, err := NewRedbox(Options{
    Schema:   "schema",
    Table:    "table",
    S3Bucket: "bucket-with-user-access",
    Truncate: false,
    RedshiftConfiguration: RedshiftConfiguration{
      Port:     "5439",
      Host:     "redshift@host.com",
      User:     "me",
      Password: "mySecret",
      Database: "myDB",
    },
  })
  handleError(err)
  
  // Data Transfer
  dataStore := getSomeDataStore()
  for dataStore.Iter() {
    rowData := dataStore.GetNextRow()
    rowBytes, _ := json.Marshal(rowData)
    handleError(redbox.Pack(rowBytes))
  }

  manifests, err := redbox.Ship()
  handleError(err)

  log.Printf("Data is located at these manifests %+v", manifests)
}
