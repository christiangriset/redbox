package redbox

import (
	"bytes"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"

	yaml "gopkg.in/yaml.v2"
)

var (
	supportedColumnTypes = []string{"boolean", "float", "int", "timestamp", "text", "longtext"}

	// Granularity configurations
	supportedGranularityDurations = []time.Duration{time.Hour, 24 * time.Hour}
	supportedGranularities        = []string{"hour", "day"}
	durationToGranularity         = map[time.Duration]string{
		time.Hour:      "hour",
		24 * time.Hour: "day",
	}

	// ErrIncompleteTableName indicates either a schema or table name weren't provided
	ErrIncompleteTableName = fmt.Errorf("Must provide both a schema and table name.")

	// ErrInvalidDataTimestamp indicates no column represents the timestamp of the data
	ErrInvalidDataTimestamp = fmt.Errorf("A data timestamp column must be provided.")

	// ErrMultipleDistKeys indicates non-uniqueness of dist-keys
	ErrMultipleDistKeys = fmt.Errorf("Cannot have more than one distribution key.")

	// ErrInvalidSortOrds indicates sort columns aren't of the form 1, 2, 3...
	ErrInvalidSortOrds = fmt.Errorf("Sort ordinals must occur in some ascending order, e.g. 1, 2, 3, 4...")

	// ErrUnsupportedType indicates an unknown column type
	ErrUnsupportedType = fmt.Errorf("Unknown type. Supported types are (%s)", strings.Join(supportedColumnTypes, ", "))

	// ErrUnsupportedGranularity indicates the input granularity isn't supported
	ErrUnsupportedGranularity = fmt.Errorf("Unsupported granularity, only support (%s)", strings.Join(supportedGranularities, ", "))
)

// Convenience function for determining if a string is in a list. Copied from http://stackoverflow.com/questions/10485743/contains-method-for-a-slice.
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// S3ToRedshiftConfig supports a Redbox with an s3-to-Redshift hookup.
// If provided, a NewRedbox setup will automically fill in the full S3ToRedshift
// object.
type S3ToRedshiftConfig struct {
	// JobEndpoint points to location where to send a job
	JobEndpoint string

	// Columns are the column names and configuration
	Columns []Column
}

// S3ToRedshift details the full configuration of the destination table. Note that if
// the provided configuration doesn't match an existing table the s3-to-Redshift job will error.
type S3ToRedshift struct {
	// JobEndpoint points to location where to send a job
	JobEndpoint string

	// S3Bucket is the s3 bucket s3-to-Redshift will look for data.
	S3Bucket string

	// S3Handler manages s3 calls
	S3Handler *s3.S3

	// Schema is the destination schema/namespace
	Schema string

	// Table is the destination table
	Table string

	// Columns are the column names and configuration
	Columns []Column

	// DataTimestampColumn tells us which column should be used as a timestamp. This is crucial for
	// s3-to-Redshift's safety guard against accidentally duplicating data.
	DataTimestampColumn string

	// Granularity indicates how often we expect new data
	Granularity time.Duration

	// Truncate indicates whether we should clear the table before adding new data
	Truncate bool

	// Force indates whether we should override the data protection
	Force bool
}

// Column is the full configuration around a column
type Column struct {
	// Name is the name of the destination column
	Name string `yaml:"dest"`

	// Type is the data type, e.g. int, text
	Type string `yaml:"type"`

	// SortOrd indicates sorting order of the column. 1 indicates sorting by this column first, 2 second, etc.
	SortOrd int `yaml:"sortord,omitempty"`

	// DistKey indicates to be used as the sharing key. There can only be one dist-key
	DistKey bool `yaml:"distkey,omitempty"`

	// DefaultVal is the default value instead of NULL
	DefaultVal string `yaml:"defaultval,omitempty"`

	// NotNull constrains the column to never have NULL values
	NotNull bool `yaml:"notnull,omitempty"`

	// PrimaryKey is the key indicating uniqueness. This also helps Redshift build query plans
	PrimaryKey bool `yaml:"primarykey,omitempty"`
}

// Validate checks if the configuration is in a valid state.
func (s *S3ToRedshift) Validate() error {
	if s.Table == "" || s.Schema == "" {
		return ErrIncompleteTableName
	}
	if s.DataTimestampColumn == "" {
		return ErrInvalidDataTimestamp
	}
	if _, ok := durationToGranularity[s.Granularity]; !ok {
		return ErrUnsupportedGranularity
	}

	nDistKeys := 0
	var timestampCols []string
	var sortOrds sort.IntSlice
	for _, col := range s.Columns {
		if col.DistKey {
			nDistKeys++
		}
		if nDistKeys > 1 {
			return ErrMultipleDistKeys
		}
		if col.SortOrd > 0 {
			sortOrds = append(sortOrds, col.SortOrd)
		}
		if col.Type == "timestamp" {
			timestampCols = append(timestampCols, col.Name)
		}
		if !contains(supportedColumnTypes, col.Type) {
			return ErrUnsupportedType
		}
	}

	sortOrds.Sort()
	for i, ord := range sortOrds {
		if i+1 != ord {
			return ErrInvalidSortOrds
		}
	}

	if !contains(timestampCols, s.DataTimestampColumn) {
		return ErrInvalidDataTimestamp
	}

	return nil
}

// GeneratePayload creates the s3-to-Redshift payload given the underlying
// object and input params.
func (s *S3ToRedshift) GeneratePayload(date time.Time) string {
	dateString := date.Format(time.RFC3339)
	payload := fmt.Sprintf("--bucket=%s --schema=%s --tables=%s --date=%s --gzip --granularity %s",
		s.S3Bucket, s.Schema, s.Table, dateString, durationToGranularity[s.Granularity])
	if s.Truncate {
		payload += " --truncate"
	}
	if s.Force {
		payload += " --force"
	}
	return payload
}

// UploadConfig uploads the config file required for s3-to-Redshift
func (s *S3ToRedshift) UploadConfig(date time.Time) error {
	dateString := date.Format(time.RFC3339)
	configPath := fmt.Sprintf("config_%s_%s_%s.yml", s.Schema, s.Table, dateString)
	configBytes := s.GenerateConfigBytes()
	return writeToS3(s.S3Handler, s.S3Bucket, configPath, configBytes)
}

// SendJob sends an s3-to-Redshift job to the provided endpoint
func (s *S3ToRedshift) SendJob(date time.Time) error {
	if err := s.UploadConfig(date); err != nil {
		return err
	}
	payload := s.GeneratePayload(date)
	_, err := http.Post(s.JobEndpoint, "text/plain", bytes.NewReader([]byte(payload)))
	return err
}

// tableMeta is used by s3 to Redshift to prevent data duplication.
type tableMeta struct {
	DataDateColumn string `yaml:"datadatecolumn"`
	Schema         string `yaml:"schema"`
}

// configYAML fully expresses the destination table in Postgres.
type configYAML struct {
	Table   string    `yaml:"dest"`
	Columns []Column  `yaml:"columns"`
	Meta    tableMeta `yaml:"meta"`
}

// GenerateConfigBytes generates the configuration file used by s3-to-Redshift
func (s *S3ToRedshift) GenerateConfigBytes() []byte {
	metaData := tableMeta{
		DataDateColumn: s.DataTimestampColumn,
		Schema:         s.Schema,
	}

	configTable := configYAML{
		Table:   s.Table,
		Columns: s.Columns,
		Meta:    metaData,
	}

	configMap := make(map[string]configYAML)
	configMap[s.Table] = configTable
	configBytes, _ := yaml.Marshal(&configMap)

	return configBytes
}
