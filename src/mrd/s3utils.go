package mrd

import (
	"bytes"
	"io"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3Config holds S3 configuration
type S3Config struct {
	BucketName string
	Region     string
}

var s3Config S3Config

// InitS3Config initializes S3 configuration
func InitS3Config(bucketName, region string) {
	s3Config.BucketName = bucketName
	s3Config.Region = region
}

// GetS3Client returns an S3 client
func GetS3Client() *s3.S3 {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(s3Config.Region),
	})
	if err != nil {
		log.Fatalf("Failed to create AWS session: %v", err)
	}
	return s3.New(sess)
}

// ReadFileFromS3 reads a file from S3 and returns its content
func ReadFileFromS3(key string) ([]byte, error) {
	s3Client := GetS3Client()
	result, err := s3Client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3Config.BucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer result.Body.Close()

	return io.ReadAll(result.Body)
}

// WriteFileToS3 writes data to an S3 object
func WriteFileToS3(key string, data []byte) error {
	s3Client := GetS3Client()
	_, err := s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s3Config.BucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

// ListFilesInS3 lists files with the given prefix
func ListFilesInS3(prefix string) ([]string, error) {
	s3Client := GetS3Client()
	result, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(s3Config.BucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	var keys []string
	for _, item := range result.Contents {
		keys = append(keys, *item.Key)
	}
	return keys, nil
}

// S3PathToLocal creates a temp file from an S3 path and returns the local path
func S3PathToLocal(s3Path string) (string, error) {
	data, err := ReadFileFromS3(s3Path)
	if err != nil {
		return "", err
	}

	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "mr-*")
	if err != nil {
		return "", err
	}

	if _, err := tmpFile.Write(data); err != nil {
		return "", err
	}

	if err := tmpFile.Close(); err != nil {
		return "", err
	}

	return tmpFile.Name(), nil
}

// LocalPathToS3 uploads a local file to S3
func LocalPathToS3(localPath, s3Path string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}

	return WriteFileToS3(s3Path, data)
}
