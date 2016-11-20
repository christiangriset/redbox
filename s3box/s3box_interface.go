package s3box

// API establishes an S3Box interface
type API interface {
	Pack(data []byte) error
	CreateManifests(manifestSlug string, nManifests int) ([]string, error)
}
