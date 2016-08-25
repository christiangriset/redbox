package redbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	schema              = "test"
	table               = "test"
	dataTimestampColumn = "time"
)

func TestValidConfig(t *testing.T) {
	assert := assert.New(t)
	dc := DestinationConfig{
		Schema: schema,
		Table:  table,
		Columns: []Column{
			Column{Name: "time", Type: "timestamp", SortOrd: 1},
			Column{Name: "id", Type: "text", DistKey: true, SortOrd: 2},
			Column{Name: "name", Type: "text", SortOrd: 3},
			Column{Name: "age", Type: "int", NotNull: true},
		},
		DataTimestampColumn: dataTimestampColumn,
	}
	assert.NoError(dc.Validate())
}

func TestInvalidConfigWithoutSchemaAndTable(t *testing.T) {
	assert := assert.New(t)
	dc := DestinationConfig{
		Schema: schema,
		Columns: []Column{
			Column{Name: "time", Type: "timestamp", SortOrd: 1},
			Column{Name: "id", Type: "text", DistKey: true, SortOrd: 2},
		},
		DataTimestampColumn: dataTimestampColumn,
	}
	assert.Equal(dc.Validate(), ErrIncompleteTableName)

	dc = DestinationConfig{
		Table: schema,
		Columns: []Column{
			Column{Name: "time", Type: "timestamp", SortOrd: 1},
			Column{Name: "id", Type: "text", DistKey: true, SortOrd: 2},
		},
		DataTimestampColumn: dataTimestampColumn,
	}
	assert.Equal(dc.Validate(), ErrIncompleteTableName)

}

func TestInvalidConfigWithoutInvalidDataTimestamp(t *testing.T) {
	assert := assert.New(t)
	dc := DestinationConfig{
		Schema: schema,
		Table:  table,
		Columns: []Column{
			Column{Name: "time", Type: "timestamp", SortOrd: 1},
			Column{Name: "id", Type: "text", DistKey: true, SortOrd: 2},
		},
	}
	assert.Equal(dc.Validate(), ErrInvalidDataTimestamp)
}

func TestInvalidConfigWithMultipleDistKeys(t *testing.T) {
	assert := assert.New(t)
	dc := DestinationConfig{
		Schema: schema,
		Table:  table,
		Columns: []Column{
			Column{Name: "time", Type: "timestamp"},
			Column{Name: "id", Type: "text", DistKey: true},
			Column{Name: "name", Type: "text", DistKey: true},
		},
		DataTimestampColumn: "time",
	}
	assert.Equal(dc.Validate(), ErrMultipleDistKeys)
}

func TestInvalidConfigWithInvalidSortOrd(t *testing.T) {
	assert := assert.New(t)
	dc := DestinationConfig{
		Schema: schema,
		Table:  table,
		Columns: []Column{
			Column{Name: "time", Type: "timestamp", SortOrd: 1},
			Column{Name: "id", Type: "text", SortOrd: 1},
			Column{Name: "name", Type: "text", SortOrd: 1},
		},
		DataTimestampColumn: "time",
	}
	assert.Equal(dc.Validate(), ErrInvalidSortOrds)
}

func TestInvalidConfigWithUnsupportedTypes(t *testing.T) {
	assert := assert.New(t)
	unsupportedTypes := []string{"foo", "bar", "something", "nothing"}
	for _, t := range unsupportedTypes {
		dc := DestinationConfig{
			Schema: schema,
			Table:  table,
			Columns: []Column{
				Column{Name: "time", Type: "timestamp"},
				Column{Name: "id", Type: "text"},
				Column{Name: "name", Type: "text"},
				Column{Name: "unsupported", Type: t},
			},
			DataTimestampColumn: "time",
		}
		assert.Equal(dc.Validate(), ErrUnsupportedType)
	}
}
