package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"mime"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type Job struct {
	id         int
	s3Bucket   string
	objectPath string
}
type Result struct {
	job         Job
	objectPath  string
	contentType string
}

var (
	jobs        = make(chan Job, 2000)
	results     = make(chan Result, 2000)
	concurrency = flag.Int("c", 4, "The concurrent number of workers to start.")
	delay       = flag.Int("d", 1000, "The millisecond delay between starting workers.")
	s3Bucket    = flag.String("s", "", "The s3 bucket to copy within.")
)

func worker(wg *sync.WaitGroup, s3svc *s3.S3) {
	for job := range jobs {
		t, err := copyWithinS3(job.s3Bucket, job.objectPath, s3svc)
		//fmt.Println(resultoutput)
		if err != nil {
			log.Print("(id " + strconv.Itoa(job.id) + ") - [" + job.objectPath + "] - ERROR: " + err.Error())
		}
		result := Result{job: job, objectPath: job.objectPath, contentType: t}
		results <- result
	}
	wg.Done()
}

func createWorkerPool(noOfWorkers int, s3svc *s3.S3) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg, s3svc)
		// Ramp up the number of workers to slow the initial request rate down
		time.Sleep(time.Duration(*delay) * time.Millisecond)
	}
	wg.Wait()
	close(results)
}

func result(done chan bool) {
	for result := range results {
		log.Print("(id " + strconv.Itoa(result.job.id) + ") - [" + result.objectPath + "] - ContentType: " + result.contentType)
	}
	done <- true
}

func main() {
	log.SetOutput(os.Stdout)
	flag.Parse()
	startTime := time.Now()
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		counter := 0
		var stripre = regexp.MustCompile(`^\d+`)
		for scanner.Scan() {
			line := stripre.ReplaceAllString(scanner.Text(), `$1`)
			jobs <- Job{id: counter, s3Bucket: *s3Bucket, objectPath: line}
			counter++
		}
		close(jobs)
	}()

	done := make(chan bool)
	go result(done)

	creds := credentials.NewEnvCredentials()

	_, err := creds.Get()
	if err != nil {
		panic(err)
	}

	cfg := aws.NewConfig().WithRegion("us-east-1").WithCredentials(creds)

	svc := s3.New(session.New(), cfg)

	createWorkerPool(*concurrency, svc)
	<-done
	endTime := time.Now()
	diff := endTime.Sub(startTime)
	fmt.Println("total time taken ", diff.Seconds(), "seconds")
}

func copyWithinS3(s3bucket string, objectpath string, s3svc *s3.S3) (t string, err error) {
	objectpathAsUrl, err := url.Parse(objectpath)
	if err != nil {
		return "", errors.New("Failed to parse")
	}
	var filename string = path.Base(objectpath)
	var contenttype string = mime.TypeByExtension(filepath.Ext(filename))

	input := &s3.CopyObjectInput{
		Bucket:            aws.String(s3bucket),
		Key:               aws.String(objectpath),
		CopySource:        aws.String(s3bucket + objectpathAsUrl.String()),
		ContentType:       aws.String(contenttype),
		MetadataDirective: aws.String("REPLACE"),
	}

	//fmt.Println(input)

	_, err = s3svc.CopyObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and
			// Message from an error.
			fmt.Println(err.Error())
		}
		return "", err
	}

	return contenttype, nil
}
