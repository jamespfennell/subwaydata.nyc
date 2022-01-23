package config

// Config describes the configuration for the ETL pipeline.
type Config struct {
	// URL of the Git repository the project metadata is in.
	GitUrl string

	// Username to use to access the Git respository.
	GitUser string

	// Email address to associate with commits made to the Git repository.
	GitEmail string

	// Password to use to access the Git respository.
	GitPassword string

	// Branch to make metadata changes on. If empty, will default to main.
	GitBranch string

	// Path within the respository to the JSON metadata file.
	MetadataPath string

	// Timezone to use.
	Timezone string

	// URL of the remote object storage service hosting the bucket.
	BucketUrl string

	// Access key for the bucket.
	BucketAccessKey string

	// Secret key for the bucket.
	BucketSecretKey string

	// Name of the bucket.
	BucketName string

	// Prefix to add to the object key of all objects stored in the bucket.
	BucketPrefix string

	// Prefix to add to the file name of all objects stored in the bucket.
	RemotePrefix string
}
