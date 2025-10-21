// s3-benchmark.go
// Copyright (c) 2017 Wasabi Technology, Inc.

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Global variables
var s3_access_key, s3_secret_key, cw_access_key, cw_secret_key string
var url_host, bucket, region string
var duration_secs, threads, loops int
var object_size uint64
var running_threads, upload_count, upload_success_count, download_count, delete_count, upload_slowdown_count, download_slowdown_count, delete_slowdown_count int32
var endtime, upload_finish, download_finish, delete_finish time.Time
var enable_cloudwatch bool
var cloudwatch_namespace string
var hostname string
var host_id string
var s3_client *s3.Client
var cloudwatch_client *cloudwatch.Client

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
	creds := credentials.NewStaticCredentialsProvider(s3_access_key, s3_secret_key, "")
	
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

func getCloudWatchClient() *cloudwatch.Client {
	// Build our config
	creds := credentials.NewStaticCredentialsProvider(cw_access_key, cw_secret_key, "")
	
	// Build the rest of the configuration
	awsConfig, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(creds),
		config.WithRegion(region),
	)
	if err != nil {
		log.Fatalf("FATAL: Unable to load AWS config for CloudWatch: %v", err)
	}
	
	// Create CloudWatch client
	client := cloudwatch.NewFromConfig(awsConfig)
	
	if client == nil {
		log.Fatalf("FATAL: Unable to create CloudWatch client.")
	}
	
	return client
}

func publishIndividualMetric(client *cloudwatch.Client, metricName string, value float64, unit cwtypes.StandardUnit) {
	timestamp := time.Now()
	
	metricData := []cwtypes.MetricDatum{
		{
			MetricName: aws.String(metricName),
			Value:      aws.Float64(value),
			Unit:       unit,
			Timestamp:  aws.Time(timestamp),
			Dimensions: []cwtypes.Dimension{
				{Name: aws.String("Bucket"), Value: aws.String(bucket)},
			},
		},
	}
	
	// Publish metric to CloudWatch (fire and forget, don't block on errors)
	go func() {
		_, err := client.PutMetricData(context.TODO(), &cloudwatch.PutMetricDataInput{
			Namespace:  aws.String(cloudwatch_namespace),
			MetricData: metricData,
		})
		if err != nil {
			log.Printf("WARNING: Failed to publish individual metric %s: %v", metricName, err)
		}
	}()
}

func createMetric(name string, value float64, unit cwtypes.StandardUnit, timestamp time.Time) cwtypes.MetricDatum {
	return cwtypes.MetricDatum{
		MetricName: aws.String(name),
		Value:      aws.Float64(value),
		Unit:       unit,
		Timestamp:  aws.Time(timestamp),
		Dimensions: []cwtypes.Dimension{
			{Name: aws.String("Bucket"), Value: aws.String(bucket)},
		},
	}
}

func publishCloudWatchMetrics(loop int, putThroughput, getThroughput, putOpsPerSec, getOpsPerSec float64, putCount, getCount int32) {
	if !enable_cloudwatch || cloudwatch_client == nil {
		return
	}
	
	timestamp := time.Now()

	// Create metric data using helper function
	metricData := []cwtypes.MetricDatum{
		createMetric("PutThroughput", putThroughput/1024/1024, cwtypes.StandardUnitMegabytesSecond, timestamp),
		createMetric("GetThroughput", getThroughput/1024/1024, cwtypes.StandardUnitMegabytesSecond, timestamp),
		createMetric("PutOpsPerSecond", putOpsPerSec, cwtypes.StandardUnitCountSecond, timestamp),
		createMetric("GetOpsPerSecond", getOpsPerSec, cwtypes.StandardUnitCountSecond, timestamp),
		createMetric("PutObjectCount", float64(putCount), cwtypes.StandardUnitCount, timestamp),
		createMetric("GetObjectCount", float64(getCount), cwtypes.StandardUnitCount, timestamp),
	}
	
	// Publish metrics to CloudWatch
	_, err := cloudwatch_client.PutMetricData(context.TODO(), &cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(cloudwatch_namespace),
		MetricData: metricData,
	})
	
	if err != nil {
		log.Printf("WARNING: Failed to publish CloudWatch metrics: %v", err)
	} else {
		log.Printf("Successfully published aggregate metrics to CloudWatch namespace: %s", cloudwatch_namespace)
	}
}

func createBucket(ignore_errors bool) {
	// Create our bucket (may already exist without error)
	in := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if _, err := s3_client.CreateBucket(context.TODO(), in); err != nil {
		if ignore_errors {
			log.Printf("WARNING: createBucket %s error, ignoring %v", bucket, err)
		} else {
			log.Fatalf("FATAL: Unable to create bucket %s (is your access and secret correct?): %v", bucket, err)
		}
	}
}

func deleteAllObjects() {
	// Use multiple routines to do the actual delete
	var doneDeletes sync.WaitGroup
	// Loop deleting our versions reading as big a list as we can
	var keyMarker, versionId *string
	var err error
	for loop := 1; ; loop++ {
		// Delete all the existing objects and versions in the bucket
		in := &s3.ListObjectVersionsInput{Bucket: aws.String(bucket), KeyMarker: keyMarker, VersionIdMarker: versionId, MaxKeys: aws.Int32(1000)}
		if listVersions, listErr := s3_client.ListObjectVersions(context.TODO(), in); listErr == nil {
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
					if _, e := s3_client.DeleteObjects(context.TODO(), deleteInput); e != nil {
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


func runUpload(thread_num int, loopnum int) {
	for time.Now().Before(endtime) {
		objnum := atomic.AddInt32(&upload_count, 1)
		key := fmt.Sprintf("Object-%d-%s-%d", objnum, host_id, loopnum)
		
		// Generate unique random data for each object
		unique_data := make([]byte, object_size)
		rand.Read(unique_data)
		
		// Track operation time
		opStart := time.Now()
		
		_, err := s3_client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(unique_data),
		})
		
		opDuration := time.Since(opStart).Seconds()
		
		if err != nil {
			// Check for rate limiting and temporary errors
			errStr := err.Error()
			if strings.Contains(errStr, "ServiceUnavailable") || 
			   strings.Contains(errStr, "SlowDown") ||
			   strings.Contains(errStr, "Throttling") ||
			   strings.Contains(errStr, "RequestTimeout") ||
			   strings.Contains(errStr, "retry quota exceeded") ||
			   strings.Contains(errStr, "RequestTimeTooSkewed") {
				atomic.AddInt32(&upload_slowdown_count, 1)
				// Don't decrement upload_count - keep object number to avoid gaps
				// Log occasional rate limit errors
				if upload_slowdown_count%100 == 1 {
					log.Printf("[UPLOAD RATE LIMIT] %d upload slowdowns so far - continuing benchmark...", upload_slowdown_count)
				}
			} else {
				log.Fatalf("FATAL: Error uploading object %s: %v", key, err)
			}
		} else {
			// Track successful uploads
			atomic.AddInt32(&upload_success_count, 1)
			
			if enable_cloudwatch && cloudwatch_client != nil {
				// Publish individual upload metrics
				throughput := float64(object_size) / opDuration // Convert to MB/s
				// publishIndividualMetric(cloudwatch_client, "PutLatency", opDuration*1000, cwtypes.StandardUnitMilliseconds)
				publishIndividualMetric(cloudwatch_client, "PutThroughputIndividual", throughput, cwtypes.StandardUnitBytesSecond)
				publishIndividualMetric(cloudwatch_client, "PutBytes", float64(object_size), cwtypes.StandardUnitBytes)
			}
		}
	}
	// Remember last done time
	upload_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func runDownload(thread_num int, loopnum int) {
	for time.Now().Before(endtime) {

		// Wait for successful uploads before downloading
		// Use upload_count (total attempts) not upload_success_count
		// This ensures we try to download all attempted object numbers
		currentCount := atomic.LoadInt32(&upload_count)
		if currentCount == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		atomic.AddInt32(&download_count, 1)
		objnum := rand.Int31n(currentCount) + 1
		key := fmt.Sprintf("Object-%d-%s-%d", objnum, host_id, loopnum)
		
		// Track operation time
		opStart := time.Now()
		
		result, err := s3_client.GetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		
		if err != nil {
			errStr := err.Error()
			if strings.Contains(errStr, "ServiceUnavailable") || 
			   strings.Contains(errStr, "SlowDown") ||
			   strings.Contains(errStr, "Throttling") ||
			   strings.Contains(errStr, "RequestTimeout") ||
			   strings.Contains(errStr, "retry quota exceeded") {
				atomic.AddInt32(&download_slowdown_count, 1)
				atomic.AddInt32(&download_count, -1)
				// Log occasional rate limit errors
				if download_slowdown_count%100 == 1 {
					log.Printf("[DOWNLOAD RATE LIMIT] %d download slowdowns so far - continuing benchmark...", download_slowdown_count)
				}
			} else if strings.Contains(errStr, "NoSuchKey") {
				atomic.AddInt32(&download_slowdown_count, 1)
				atomic.AddInt32(&download_count, -1)
				log.Printf("Object %s not found (upload_count=%d, objnum=%d, host_id='%s'), skipping...", key, upload_count, objnum, host_id)
			} else {
				log.Fatalf("FATAL: Error downloading object %s: %v", key, err)
			}
		} else if result.Body != nil {
			io.Copy(ioutil.Discard, result.Body)
			result.Body.Close()
			
			opDuration := time.Since(opStart).Seconds()
			
			if enable_cloudwatch && cloudwatch_client != nil {
				// Publish individual download metrics
				throughput := float64(object_size) / opDuration// Convert to MB/s
				// publishIndividualMetric(cloudwatch_client, "GetLatency", opDuration*1000, cwtypes.StandardUnitMilliseconds)
				publishIndividualMetric(cloudwatch_client, "GetThroughputIndividual", throughput, cwtypes.StandardUnitBytesSecond)
				publishIndividualMetric(cloudwatch_client, "GetBytes", float64(object_size), cwtypes.StandardUnitBytes)
			}
		}
	}
	// Remember last done time
	download_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func runDelete(thread_num int, loopnum int) {
	for {
		objnum := atomic.AddInt32(&delete_count, 1)
		if objnum > upload_count {
			break
		}
		key := fmt.Sprintf("Object-%d-%s-%d", objnum, host_id, loopnum)
		
		_, err := s3_client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
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
	myflag.StringVar(&s3_access_key, "a", "", "S3 Access key (or set AWS_ACCESS_KEY_ID env var)")
	myflag.StringVar(&s3_secret_key, "s", "", "S3 Secret key (or set AWS_SECRET_ACCESS_KEY env var)")
	myflag.StringVar(&cw_access_key, "cwa", "", "CloudWatch Access key (defaults to S3 credentials or AWS_CW_ACCESS_KEY_ID env var)")
	myflag.StringVar(&cw_secret_key, "cws", "", "CloudWatch Secret key (defaults to S3 credentials or AWS_CW_SECRET_ACCESS_KEY env var)")
	myflag.StringVar(&url_host, "u", "https://s3.us-east-1.amazonaws.com", "URL for host with method prefix")
	myflag.StringVar(&bucket, "b", "s3-benchmark-bucket", "Bucket for testing")
	myflag.StringVar(&region, "r", "us-east-1", "Region for testing")
	myflag.IntVar(&duration_secs, "d", 60, "Duration of each test in seconds")
	myflag.IntVar(&threads, "t", 1, "Number of threads to run")
	myflag.IntVar(&loops, "l", 1, "Number of times to repeat test")
	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "1M", "Size of objects in bytes with postfix K, M, and G")
	myflag.BoolVar(&enable_cloudwatch, "cw", false, "Enable CloudWatch metrics publishing")
	myflag.StringVar(&cloudwatch_namespace, "cwns", "S3Benchmark", "CloudWatch namespace for metrics")
	myflag.StringVar(&host_id, "hostid", "", "Host identifier for multi-host testing (default: auto-generated)")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	// Load credentials from environment variables if not provided via flags
	if s3_access_key == "" {
		s3_access_key = os.Getenv("AWS_ACCESS_KEY_ID")
	}
	if s3_secret_key == "" {
		s3_secret_key = os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	
	// CloudWatch credentials default to S3 credentials if not specified
	if cw_access_key == "" {
		cw_access_key = os.Getenv("AWS_CW_ACCESS_KEY_ID")
		if cw_access_key == "" {
			cw_access_key = s3_access_key // Use S3 credentials as fallback
		}
	}
	if cw_secret_key == "" {
		cw_secret_key = os.Getenv("AWS_CW_SECRET_ACCESS_KEY")
		if cw_secret_key == "" {
			cw_secret_key = s3_secret_key // Use S3 credentials as fallback
		}
	}

	// Check the arguments
	if s3_access_key == "" {
		log.Fatal("Missing S3 access key. Provide via -a flag or AWS_ACCESS_KEY_ID environment variable.")
	}
	if s3_secret_key == "" {
		log.Fatal("Missing S3go secret key. Provide via -s flag or AWS_SECRET_ACCESS_KEY environment variable.")
	}
	if enable_cloudwatch && cw_access_key == "" {
		log.Fatal("Missing CloudWatch access key. Provide via -cwa flag or AWS_CW_ACCESS_KEY_ID environment variable.")
	}
	if enable_cloudwatch && cw_secret_key == "" {
		log.Fatal("Missing CloudWatch secret key. Provide via -cws flag or AWS_CW_SECRET_ACCESS_KEY environment variable.")
	}
	var err error
	if object_size, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}

	// Get hostname for CloudWatch dimensions
	hostname, err = os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	
	// Set host_id for object naming (use provided value or generate one)
	if host_id == "" {
		// Auto-generate a simple numeric ID based on timestamp
		host_id = fmt.Sprintf("h%d", time.Now().Unix()%10000)
	}

	// Echo the parameters
	logit(fmt.Sprintf("Parameters: url=%s, bucket=%s, region=%s, duration=%d, threads=%d, loops=%d, size=%s, cloudwatch=%v, namespace=%s, host=%s, hostid=%s",
		url_host, bucket, region, duration_secs, threads, loops, sizeArg, enable_cloudwatch, cloudwatch_namespace, hostname, host_id))

	// Initialize S3 client (create once, reuse for all operations)
	s3_client = getS3Client()
	log.Printf("S3 client initialized for %s", url_host)

	// Initialize CloudWatch client if enabled (create once, reuse for all operations)
	if enable_cloudwatch {
		cloudwatch_client = getCloudWatchClient()
		log.Printf("CloudWatch metrics enabled - namespace: %s, host: %s", cloudwatch_namespace, hostname)
	}

	// Create the bucket and delete all the objects
	createBucket(true)
	deleteAllObjects()

	// Loop running the tests
	for loop := 1; loop <= loops; loop++ {

		// reset counters
		upload_count = 0
		upload_success_count = 0
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
			go runUpload(n, loop)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		upload_time := upload_finish.Sub(starttime).Seconds()

		bps := float64(uint64(upload_success_count)*object_size) / upload_time
		logit(fmt.Sprintf("Loop %d: PUT time %.1f secs, objects = %d (attempted: %d), speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
			loop, upload_time, upload_success_count, upload_count, bytefmt.ByteSize(uint64(bps)), float64(upload_success_count)/upload_time, upload_slowdown_count))
		
		if upload_success_count < upload_count {
			logit(fmt.Sprintf("WARNING: %d uploads attempted but only %d succeeded! Missing objects: %d",
				upload_count, upload_success_count, upload_count-upload_success_count))
		}

		// Wait a bit for S3 eventual consistency
		log.Printf("Waiting 2 seconds for S3 consistency before downloads...")
		time.Sleep(2 * time.Second)

		// Run the download case
		running_threads = int32(threads)
		starttime = time.Now()
		endtime = starttime.Add(time.Second * time.Duration(duration_secs))
		for n := 1; n <= threads; n++ {
			go runDownload(n, loop)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		download_time := download_finish.Sub(starttime).Seconds()

		bps_download := float64(uint64(download_count)*object_size) / download_time
		logit(fmt.Sprintf("Loop %d: GET time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
			loop, download_time, download_count, bytefmt.ByteSize(uint64(bps_download)), float64(download_count)/download_time, download_slowdown_count))

		// Publish metrics to CloudWatch
		bps_upload := float64(uint64(upload_success_count)*object_size) / upload_time
		publishCloudWatchMetrics(loop, bps_upload, bps_download, float64(upload_success_count)/upload_time, float64(download_count)/download_time, upload_success_count, download_count)

		// Run the delete case
		running_threads = int32(threads)
		starttime = time.Now()
		endtime = starttime.Add(time.Second * time.Duration(duration_secs))
		for n := 1; n <= threads; n++ {
			go runDelete(n, loop)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		delete_time := delete_finish.Sub(starttime).Seconds()

		logit(fmt.Sprintf("Loop %d: DELETE time %.1f secs, %.1f deletes/sec. Slowdowns = %d",
			loop, delete_time, float64(upload_count)/delete_time, delete_slowdown_count))
		
		// Summary of this loop with separate rate limit metrics
		total_upload_attempts := upload_count
		total_upload_success := upload_success_count
		total_upload_rate_limited := upload_slowdown_count
		total_download_attempts := download_count + download_slowdown_count
		total_download_success := download_count
		total_download_rate_limited := download_slowdown_count
		
		upload_rate_limit_pct := float64(0)
		if total_upload_attempts > 0 {
			upload_rate_limit_pct = float64(total_upload_rate_limited) / float64(total_upload_attempts) * 100
		}
		download_rate_limit_pct := float64(0)
		if total_download_attempts > 0 {
			download_rate_limit_pct = float64(total_download_rate_limited) / float64(total_download_attempts) * 100
		}
		
		logit(fmt.Sprintf("Loop %d Summary:",loop))
		logit(fmt.Sprintf("  UPLOAD:   %d/%d succeeded, %d rate limited (%.1f%%)",
			total_upload_success, total_upload_attempts, total_upload_rate_limited, upload_rate_limit_pct))
		logit(fmt.Sprintf("  DOWNLOAD: %d/%d succeeded, %d rate limited (%.1f%%)",
			total_download_success, total_download_attempts, total_download_rate_limited, download_rate_limit_pct))
	}

	// All done
}
