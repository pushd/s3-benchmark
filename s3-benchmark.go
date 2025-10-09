// s3-benchmark.go
// Copyright (c) 2017 Wasabi Technology, Inc.

package main

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"code.cloudfoundry.org/bytefmt"
	"context"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"strings"
	"sync/atomic"
	"time"
)

// Global variables
var access_key, secret_key, url_host, bucket, region string
var duration_secs, threads, loops int
var object_size uint64
var object_data []byte
var object_data_md5 string
var running_threads, upload_count, download_count, delete_count, upload_slowdown_count, download_slowdown_count, delete_slowdown_count int32
var endtime, upload_finish, download_finish, delete_finish time.Time

func logit(msg string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile("benchmark.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(time.Now().Format(http.TimeFormat) + ": " + msg + "\n")
		logfile.Close()
	}
}

// Our HTTP transport used for the roundtripper below
var HTTPTransport http.RoundTripper = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	Dial: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}).Dial,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 0,
	// Allow an unlimited number of idle connections
	MaxIdleConnsPerHost: 4096,
	MaxIdleConns:        0,
	// But limit their idle time
	IdleConnTimeout: time.Minute,
	// Ignore TLS errors
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

var httpClient = &http.Client{Transport: HTTPTransport}

func getS3Client() *s3.Client {
	// Build our config
	creds := credentials.NewStaticCredentialsProvider(access_key, secret_key, "")
	
	// Build the rest of the configuration
	awsConfig, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(creds),
		config.WithRegion(region),
	)
	if err != nil {
		log.Fatalf("FATAL: Unable to load AWS config: %v", err)
	}
	
	// Create S3 client with custom endpoint
        client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(url_host)
		o.UsePathStyle = true
		o.HTTPClient = &http.Client{Transport: HTTPTransport}
	})
	
	if client == nil {
		log.Fatalf("FATAL: Unable to create new client.")
	}
	// Return success
	return client
}

func createBucket(ignore_errors bool) {
	// Get a client
	client := getS3Client()
	// Create our bucket (may already exist without error)
	in := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if _, err := client.CreateBucket(context.TODO(), in); err != nil {
		if ignore_errors {
			log.Printf("WARNING: createBucket %s error, ignoring %v", bucket, err)
		} else {
			log.Fatalf("FATAL: Unable to create bucket %s (is your access and secret correct?): %v", bucket, err)
		}
	}
}

func deleteAllObjects() {
	// Get a client
	client := getS3Client()
	// Use multiple routines to do the actual delete
	var doneDeletes sync.WaitGroup
	// Loop deleting our versions reading as big a list as we can
	var keyMarker, versionId *string
	var err error
	for loop := 1; ; loop++ {
		// Delete all the existing objects and versions in the bucket
		in := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), KeyMarker: keyMarker, VersionIdMarker: versionId, MaxKeys: aws.Int32(1000)}
		if listVersions, listErr := client.ListObjectVersions(context.TODO(), in); listErr == nil {
			delete := &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Quiet: aws.Bool(true)}}
			for _, version := range listVersions.Versions {
				delete.Delete.Objects = append(delete.Delete.Objects, types.ObjectIdentifier{Key: version.Key, VersionId: version.VersionId})
			}
			for _, marker := range listVersions.DeleteMarkers {
				delete.Delete.Objects = append(delete.Delete.Objects, types.ObjectIdentifier{Key: marker.Key, VersionId: marker.VersionId})
			}
			if len(delete.Delete.Objects) > 0 {
				// Start a delete routine
				doDelete := func(deleteInput *s3.DeleteObjectsInput) {
					if _, e := client.DeleteObjects(context.TODO(), deleteInput); e != nil {
						err = fmt.Errorf("DeleteObjects unexpected failure: %s", e.Error())
					}
					doneDeletes.Done()
				}
				doneDeletes.Add(1)
				go doDelete(delete)
			}
			// Advance to next versions
			if listVersions.IsTruncated == nil || !*listVersions.IsTruncated {
				break
			}
			keyMarker = listVersions.NextKeyMarker
			versionId = listVersions.NextVersionIdMarker
		} else {
			// The bucket may not exist, just ignore in that case
			if strings.HasPrefix(listErr.Error(), "NoSuchBucket") {
				return
			}
			err = fmt.Errorf("ListObjectVersions unexpected failure: %v", listErr)
			break
		}
	}
	// Wait for deletes to finish
	doneDeletes.Wait()
	// If error, it is fatal
	if err != nil {
		log.Fatalf("FATAL: Unable to delete objects from bucket: %v", err)
	}
}


func runUpload(thread_num int) {
	client := getS3Client()
	for time.Now().Before(endtime) {
		objnum := atomic.AddInt32(&upload_count, 1)
		key := fmt.Sprintf("Object-%d", objnum)
		
		_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(object_data),
		})
		
		if err != nil {
			if strings.Contains(err.Error(), "ServiceUnavailable") {
				atomic.AddInt32(&upload_slowdown_count, 1)
				atomic.AddInt32(&upload_count, -1)
			} else {
				log.Fatalf("FATAL: Error uploading object %s: %v", key, err)
			}
		}
	}
	// Remember last done time
	upload_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func runDownload(thread_num int) {
	client := getS3Client()
	for time.Now().Before(endtime) {
		atomic.AddInt32(&download_count, 1)
		objnum := rand.Int31n(upload_count) + 1
		key := fmt.Sprintf("Object-%d", objnum)
		
		result, err := client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		
		if err != nil {
			if strings.Contains(err.Error(), "ServiceUnavailable") {
				atomic.AddInt32(&download_slowdown_count, 1)
				atomic.AddInt32(&download_count, -1)
			} else {
				log.Fatalf("FATAL: Error downloading object %s: %v", key, err)
			}
		} else if result.Body != nil {
			io.Copy(ioutil.Discard, result.Body)
			result.Body.Close()
		}
	}
	// Remember last done time
	download_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func runDelete(thread_num int) {
	client := getS3Client()
	for {
		objnum := atomic.AddInt32(&delete_count, 1)
		if objnum > upload_count {
			break
		}
		key := fmt.Sprintf("Object-%d", objnum)
		
		_, err := client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		
		if err != nil {
			if strings.Contains(err.Error(), "ServiceUnavailable") {
				atomic.AddInt32(&delete_slowdown_count, 1)
				atomic.AddInt32(&delete_count, -1)
			} else {
				log.Fatalf("FATAL: Error deleting object %s: %v", key, err)
			}
		}
	}
	// Remember last done time
	delete_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func main() {
	// Hello
	fmt.Println("Wasabi benchmark program v2.0")

	// Parse command line
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.StringVar(&access_key, "a", "", "Access key")
	myflag.StringVar(&secret_key, "s", "", "Secret key")
	myflag.StringVar(&url_host, "u", "https://s3.us-east-005.backblazeb2.com", "URL for host with method prefix")
	myflag.StringVar(&bucket, "b", "backblaze-benchmark-bucket", "Bucket for testing")
	myflag.StringVar(&region, "r", "us-east-005", "Region for testing")
	myflag.IntVar(&duration_secs, "d", 60, "Duration of each test in seconds")
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.IntVar(&loops, "l", 1, "Number of times to repeat test")
	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with postfix K, M, and G")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	// Check the arguments
	if access_key == "" {
		log.Fatal("Missing argument -a for access key.")
	}
	if secret_key == "" {
		log.Fatal("Missing argument -s for secret key.")
	}
	var err error
	if object_size, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}

	// Echo the parameters
	logit(fmt.Sprintf("Parameters: url=%s, bucket=%s, region=%s, duration=%d, threads=%d, loops=%d, size=%s",
		url_host, bucket, region, duration_secs, threads, loops, sizeArg))

	// Initialize data for the bucket
	object_data = make([]byte, object_size)
	rand.Read(object_data)
	hasher := md5.New()
	hasher.Write(object_data)
	object_data_md5 = base64.StdEncoding.EncodeToString(hasher.Sum(nil))

	// Create the bucket and delete all the objects
	createBucket(true)
	deleteAllObjects()

	// Loop running the tests
	for loop := 1; loop <= loops; loop++ {

		// reset counters
		upload_count = 0
		upload_slowdown_count = 0
		download_count = 0
		download_slowdown_count = 0
		delete_count = 0
		delete_slowdown_count = 0

		// Run the upload case
		running_threads = int32(threads)
		starttime := time.Now()
		endtime = starttime.Add(time.Second * time.Duration(duration_secs))
		for n := 1; n <= threads; n++ {
			go runUpload(n)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		upload_time := upload_finish.Sub(starttime).Seconds()

		bps := float64(uint64(upload_count)*object_size) / upload_time
		logit(fmt.Sprintf("Loop %d: PUT time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
			loop, upload_time, upload_count, bytefmt.ByteSize(uint64(bps)), float64(upload_count)/upload_time, upload_slowdown_count))

		// Run the download case
		running_threads = int32(threads)
		starttime = time.Now()
		endtime = starttime.Add(time.Second * time.Duration(duration_secs))
		for n := 1; n <= threads; n++ {
			go runDownload(n)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		download_time := download_finish.Sub(starttime).Seconds()

		bps = float64(uint64(download_count)*object_size) / download_time
		logit(fmt.Sprintf("Loop %d: GET time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
			loop, download_time, download_count, bytefmt.ByteSize(uint64(bps)), float64(download_count)/download_time, download_slowdown_count))

		// Run the delete case
		running_threads = int32(threads)
		starttime = time.Now()
		endtime = starttime.Add(time.Second * time.Duration(duration_secs))
		for n := 1; n <= threads; n++ {
			go runDelete(n)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		delete_time := delete_finish.Sub(starttime).Seconds()

		logit(fmt.Sprintf("Loop %d: DELETE time %.1f secs, %.1f deletes/sec. Slowdowns = %d",
			loop, delete_time, float64(upload_count)/delete_time, delete_slowdown_count))
	}

	// All done
}
