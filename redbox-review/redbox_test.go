package redbox

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var (
	schema           = "test"
	table            = "test"
	s3Bucket         = "bucket"
	s3Region         = "region"
	awsKey           = "key"
	awsPassword      = "secret"
	testManifestSlug = "slug"

	testOptions = NewRedboxOptions{
		Schema:      schema,
		Table:       table,
		S3Bucket:    s3Bucket,
		S3Region:    s3Region,
		AWSKey:      awsKey,
		AWSPassword: awsPassword,
	}
)

type MockSuccessS3Box struct {
}

func (m *MockSuccessS3Box) Pack(data []byte) error {
	return nil
}

func (m *MockSuccessS3Box) CreateManifests(manifestSlug string, nManifests int) ([]string, error) {
	var manifests []string
	for i := 0; i < nManifests; i++ {
		manifest := fmt.Sprintf("%s_%d.manifest", testManifestSlug, i)
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}

type MockSlowS3Box struct {
}

func (m *MockSlowS3Box) Pack(data []byte) error {
	time.Sleep(10 * time.Millisecond)
	return nil
}

func (m *MockSlowS3Box) CreateManifests(manifestSlug string, nManifests int) ([]string, error) {
	time.Sleep(100 * time.Millisecond)
	var manifests []string
	for i := 0; i < nManifests; i++ {
		manifest := fmt.Sprintf("%s_%d.manifest", testManifestSlug, i)
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}

func TestSuccessfulJSONPack(t *testing.T) {
	assert := assert.New(t)
	s3Box := &MockSuccessS3Box{}
	redshift, mock, err := sqlmock.New()
	assert.NoError(err)
	redbox := newRedboxInjection(testOptions, s3Box, redshift)

	data, _ := json.Marshal(map[string]interface{}{"key": "value"})
	assert.NoError(redbox.Pack(data))
	assert.NoError(mock.ExpectationsWereMet()) // Assert no SQL statements were made.
}

func TestUnsuccessfulCSVPack(t *testing.T) {
	assert := assert.New(t)
	s3Box := &MockSuccessS3Box{}
	redshift, mock, err := sqlmock.New()
	assert.NoError(err)
	redbox := newRedboxInjection(testOptions, s3Box, redshift)

	data := []byte("d1,d2")
	assert.Equal(redbox.Pack(data), errInvalidJSONInput)
	assert.NoError(mock.ExpectationsWereMet()) // Assert no SQL statements were made.
}

func TestCorrectDBCallsOnSendWithTruncate(t *testing.T) {
	assert := assert.New(t)
	s3Box := &MockSuccessS3Box{}
	redshift, mock, err := sqlmock.New()
	assert.NoError(err)
	options := testOptions
	options.Truncate = true
	options.NManifests = 5
	redbox := newRedboxInjection(options, s3Box, redshift)

	// Set expected commands for mocked SQL client
	mock.ExpectBegin()
	delStmt := fmt.Sprintf("DELETE FROM \"%s\".\"%s\"", schema, table)
	mock.ExpectExec(delStmt).WillReturnResult(sqlmock.NewResult(1, 1))

	manifests, err := s3Box.CreateManifests(testManifestSlug, redbox.nManifests)
	assert.NoError(err)
	for _, manifest := range manifests {
		copyStmt := redbox.copyStatement(manifest)
		mock.ExpectExec(copyStmt).WillReturnResult(sqlmock.NewResult(1, 1))
	}

	mock.ExpectCommit()

	// Run send and assert correct calls were made
	shippedManifests, err := redbox.Ship()
	assert.Equal(shippedManifests, manifests)
	assert.NoError(err)
	assert.NoError(mock.ExpectationsWereMet())
}

func TestCorrectDBCallsOnSendWithoutTruncate(t *testing.T) {
	assert := assert.New(t)
	s3Box := &MockSuccessS3Box{}
	redshift, mock, err := sqlmock.New()
	assert.NoError(err)
	options := testOptions
	options.NManifests = 5
	redbox := newRedboxInjection(options, s3Box, redshift)

	// Set expected commands for mocked SQL client
	mock.ExpectBegin()
	manifests, err := s3Box.CreateManifests(testManifestSlug, redbox.nManifests)
	assert.NoError(err)
	for _, manifest := range manifests {
		copyStmt := redbox.copyStatement(manifest)
		mock.ExpectExec(copyStmt).WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	// Run Send and assert correct calls were made
	shippedManifests, err := redbox.Ship()
	assert.Equal(shippedManifests, manifests)
	assert.NoError(err)
	assert.NoError(mock.ExpectationsWereMet())
}

func TestRollbackOnError(t *testing.T) {
	assert := assert.New(t)
	s3Box := &MockSuccessS3Box{}
	redshift, mock, err := sqlmock.New()
	assert.NoError(err)
	options := testOptions
	options.NManifests = 5
	redbox := newRedboxInjection(options, s3Box, redshift)

	// Set expected commands for mocked SQL client
	mock.ExpectBegin()
	manifests, err := s3Box.CreateManifests(testManifestSlug, redbox.nManifests)
	assert.NoError(err)

	copyErr := fmt.Errorf("Some COPY Error")
	copyStmt := redbox.copyStatement(manifests[0])
	mock.ExpectExec(copyStmt).WillReturnError(copyErr)
	mock.ExpectRollback()

	// Run Send and assert correct calls were made
	shippedManifests, err := redbox.Ship()
	assert.Nil(shippedManifests)
	assert.Equal(err, copyErr)
	assert.NoError(mock.ExpectationsWereMet())
}

func TestNoActionWithNoDataWrites(t *testing.T) {
	assert := assert.New(t)
	s3Box := &MockSuccessS3Box{}
	redshift, mock, err := sqlmock.New()
	assert.NoError(err)
	options := testOptions
	options.NManifests = 0
	redbox := newRedboxInjection(options, s3Box, redshift)

	manifests, err := redbox.Ship()
	assert.Nil(manifests)
	assert.Equal(err, errNothingToShip)
	assert.NoError(mock.ExpectationsWereMet())
}

func TestNoActionsAllowedDuringOrAfterSuccessfulSend(t *testing.T) {
	assert := assert.New(t)
	s3Box := &MockSlowS3Box{}
	redshift, mock, err := sqlmock.New()
	assert.NoError(err)
	options := testOptions
	options.NManifests = 5
	redbox := newRedboxInjection(options, s3Box, redshift)

	// Set expected commands for mocked SQL client
	mock.ExpectBegin()
	manifests, err := s3Box.CreateManifests(testManifestSlug, redbox.nManifests)
	assert.NoError(err)
	for _, manifest := range manifests {
		copyStmt := redbox.copyStatement(manifest)
		mock.ExpectExec(copyStmt).WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	// Kick off a slow running ship
	go func() {
		_, err := redbox.Ship()
		assert.NoError(err)
	}()
	time.Sleep(10 * time.Millisecond)

	// Ensure that operations error during a send
	data, _ := json.Marshal(map[string]interface{}{"key": "value"})
	for redbox.isShippingInProgress() {
		assert.Equal(redbox.Pack(data), errShippingInProgress)
		_, shipErr := redbox.Ship()
		assert.Equal(shipErr, errShippingInProgress)
		time.Sleep(7 * time.Millisecond)
	}

	assert.Equal(redbox.Pack(data), errBoxShipped)
	_, shipErr := redbox.Ship()
	assert.Equal(shipErr, errBoxShipped)

	assert.NoError(mock.ExpectationsWereMet())
}
