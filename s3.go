package redbox

import (
	"compress/gzip"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const aesAlgo = "AES256" // Algo used for server-side encryption.

// Modularize functions for testing
var (
	getRegionForBucket func(string) (string, error)
	writeToS3          func(s3Handler *s3.S3, bucket string, fileKey string, data []byte) error
)

// getRegionForBucketProd looks up the region name for the given bucket
func getRegionForBucketProd(name string) (string, error) {
	// Any region will work for the region lookup, but the request MUST use PathStyle
	config := aws.NewConfig().WithRegion("us-west-1").WithS3ForcePathStyle(true)
	session := session.New()
	client := s3.New(session, config)
	params := s3.GetBucketLocationInput{
		Bucket: aws.String(name),
	}
	resp, err := client.GetBucketLocation(&params)
	if err != nil {
		return "", fmt.Errorf("Failed to get location for bucket '%s', %s", name, err)
	}
	if resp.LocationConstraint == nil {
		// "US Standard", returns an empty region. So return any region in the US
		return "us-east-1", nil
	}
	return *resp.LocationConstraint, nil
}

// uploadToS3 streams readers to an encrypted s3 file.
func uploadToS3(s3Handler *s3.S3, bucket, fileKey string, data io.Reader) error {
	uploader := s3manager.NewUploaderWithClient(s3Handler)
	_, err := uploader.Upload(&s3manager.UploadInput{
		Body:                 data,
		Bucket:               aws.String(bucket),
		Key:                  aws.String(fileKey),
		ServerSideEncryption: aws.String(aesAlgo),
	})
	return err
}

// compressAndWriteBytesToS3 creates a gzip compression for writing to s3.
//
// The mechanics of this function deserve some attention. AWSs upload requires a reader
// and our goal is to compress out bytes in gzip format and stream them to s3.
// We accomplish this by creating a reader/writer pair returned from io.Pipe().
// To correctly use this pair we need the reader to be hooked up to a sync before writing any data,
// thus we set off two go routines, one for hooking up the source (the data to write) and another
// for establishing the sync (the destination s3 file).
func compressAndWriteBytesToS3(s3Handler *s3.S3, bucket, key string, data []byte) error {
	var wg sync.WaitGroup
	var writeErr error
	var streamErr error
	wg.Add(2)
	reader, writer := io.Pipe()

	// Source initiation
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		gzipWriter := gzip.NewWriter(writer)
		defer writer.Close()
		defer gzipWriter.Close()
		_, writeErr = gzipWriter.Write(data)
	}(&wg)

	// Sink initiation
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		streamErr = uploadToS3(s3Handler, bucket, key, reader)
	}(&wg)
	wg.Wait()
	if writeErr != nil {
		return writeErr
	}
	return streamErr
}

func init() {
	getRegionForBucket = getRegionForBucketProd
	writeToS3 = compressAndWriteBytesToS3
}
