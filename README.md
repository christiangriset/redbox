# Redbox

Library simplifying data transport to Redshift via straighforward configuration and intuitive methods (Pack and Ship).

Redbox is transactional. User packs data until they're finished, after which a call to Ship transports the data to Redshift.

See [the Godocs](https://godoc.org/github.com/cgclever/redbox) for API documentation

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

### NewRedboxOptions

```
type NewRedboxOptions struct {
	// Required inputs
  Schema                string
  Table                 string
	S3Bucket              string
	Truncate              bool
  RedshiftConfiguration RedshiftConfiguration

  // Optional region of the S3Bucket. If not provided Redbox attempts to use 
  // the AWS API to get its location, however requires the user have permission for this action.
  S3Region string

  // Optional AWS creds. If not provided they'll be grabbed from the environment.
	AWSKey      string
	AWSPassword string
	
	// Optional management configurations.
	BufferSize int // Default: 100MB
  NManifests int // Default: 4
}
```

- Truncate instructs Redbox to clear the destination first before transporting data. This is useful for tables representing current snapshots of the world.
- BufferSize sets the file sizes uploaded s3. This is useful for memory management and `2\*BufferSize` should be comfortably available at all times. AWS recommends this lie between 10MB and 1GB.
- NManifests will not likely need to be touched. To prevent connection timeouts for extremely large data transports, NManifests will instruct how many manifests to split the data across. A default of 4 should be sufficient for a large number of use cases.


## Redbox - The Methods

### Pack(data []byte) error

Pack buffers data without sending to Redshift and is concurrency safe.

Currently Pack is a single row operation which *only* accepts JSONifiable inputs, i.e. those marshalable into a `map[string]interface{}`.

### Ship() ([]string, error)

Ship commits all packed data to Redshift. If "Truncate" is provided in the configuration, the destination table will first be deleted.
Ship is transactional, meaning any returned error implies the destination table has been left unchanged.

## Example

```
func SomeJob() {
  // Setup. AWS creds determined from environment.
  redbox, err := NewRedbox(NewRedboxOptions{
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
