package s3box

// S3BoxAPI establishes an S3Box interface
type S3BoxAPI interface {
	Pack(data []byte) error
	CreateManifests(manifestSlug string, nManifests int) ([]string, error)
	FreshBox() S3BoxAPI
}
