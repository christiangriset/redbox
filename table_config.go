package rio

import (
	"fmt"
	"sort"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

var (
	supportedColumnTypes = []string{"boolean", "float", "int", "timestamp", "text", "longtext"}

	// ErrIncompleteTableName indicates either a schema or table name weren't provided
	ErrIncompleteTableName = fmt.Errorf("Must provide both a schema and table name.")

	// ErrInvalidDataTimestamp indicates no column represents the timestamp of the data
	ErrInvalidDataTimestamp = fmt.Errorf("A data timestamp column must be provided.")

	// ErrMultipleDistKeys indicates non-uniqueness of dist-keys
	ErrMultipleDistKeys = fmt.Errorf("Cannot have more than one distribution key.")

	// ErrInvalidSortOrds indicates sort columns aren't of the form 1, 2, 3...
	ErrInvalidSortOrds = fmt.Errorf("Sort ordinals must occur in some ascending order, e.g. 1, 2, 3, 4...")

	// ErrUnsupportedType indicates an unknown column type
	ErrUnsupportedType = fmt.Errorf("Unknown type. Supported types are %s", strings.Join(supportedColumnTypes, ", "))
)

// Column is the full configuration around a column
type Column struct {
	// ColumnName is the name of the destination column
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

// DestinationConfig details the full configuration of the destination table. Note that if
// the provided configuration doesn't match an existing table the s3-to-Redshift job will error.
type DestinationConfig struct {
	// Schema is the destination schema/namespace
	Schema string

	// Table is the destination table
	Table string

	// Columns are the column names and configuration
	Columns []Column

	// DataTimestampColumn tells us which column should be used as a timestamp. This is crucial for
	// s3-to-Redshift's safety guard against accidentally duplicating data.
	DataTimestampColumn string
}

// Convenience function for determining if a string is in a list. Copied from http://stackoverflow.com/questions/10485743/contains-method-for-a-slice.
func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// Validate checks if the configuration is in a valid state.
func (dc *DestinationConfig) Validate() error {
	if dc.Table == "" || dc.Schema == "" {
		return ErrIncompleteTableName
	}
	if dc.DataTimestampColumn == "" {
		return ErrInvalidDataTimestamp
	}

	nDistKeys := 0
	var timestampCols []string
	var sortOrds sort.IntSlice
	for _, col := range dc.Columns {
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

	if !contains(timestampCols, dc.DataTimestampColumn) {
		return ErrInvalidDataTimestamp
	}

	return nil
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
func (dc *DestinationConfig) GenerateConfigBytes() []byte {
	metaData := tableMeta{
		DataDateColumn: dc.DataTimestampColumn,
		Schema:         dc.Schema,
	}

	configTable := configYAML{
		Table:   dc.Table,
		Columns: dc.Columns,
		Meta:    metaData,
	}

	configMap := make(map[string]configYAML)
	configMap[dc.Table] = configTable
	configBytes, _ := yaml.Marshal(&configMap)

	return configBytes
}
