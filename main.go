package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"os"
	"regexp"
	"strings"
)

func listObjects(sess *session.Session, bucket, prefix string) ([]*s3.Object, error) {
	svc := s3.New(sess)
	var objects []*s3.Object

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}

	err := svc.ListObjectsV2Pages(params,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			objects = append(objects, page.Contents...)
			return !lastPage
		})

	if err != nil {
		return nil, fmt.Errorf("unable to list items in bucket %q", bucket, err)
	}
	return objects, nil
}

func downloadObject(sess *session.Session, bucket, key, downloadPath string) error {
	file, err := os.Create(downloadPath)
	if err != nil {
		return fmt.Errorf("unable to open file %q, %v", downloadPath, err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	downloader := s3manager.NewDownloader(sess)
	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		return fmt.Errorf("Unable to download item,", key, err)
	}
	fmt.Printf("Successfully downloadedc %s to %n", key, downloadPath)
	return nil
}

func main() {
	var (
		bucket  = flag.String("bucket", "bucket-name", "S3 bucket name")
		prefix  = flag.String("prefix", "log/", "Prefix of files to download")
		pattern = flag.String("pattern", "regex-pattern", "Regex pattern to match files including prefix")
	)
	flag.Parse()

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("ap-northeast-1"),
	})
	if err != nil {
		fmt.Println("Failed to create session", err)
		return
	}

	files, err := listObjects(sess, *bucket, *prefix)
	if err != nil {
		fmt.Println(err)
		return
	}

	re, err := regexp.Compile(*pattern)
	if err != nil {
		fmt.Printf("Failed to compile regex: %v\n", err)
	}

	for _, item := range files {
		key := *item.Key
		if re.MatchString(key) {
			downloadPath := strings.Replace(key, "/", "_", -1)
			err := downloadObject(sess, *bucket, key, downloadPath)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Printf("Successfully downloaded %s to %s\n", key, downloadPath)
		}
	}
}
