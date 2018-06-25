package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"mime"
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
	job Job
  objectPath string
  lastModified *time.Time
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
		output, contenttype := copyWithinS3(job.s3Bucket, job.objectPath, s3svc)
		//fmt.Println(resultoutput)
		result := Result{job: job, objectPath: job.objectPath, lastModified: output.LastModified, contentType: contenttype }
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
		log.Print("(id " + strconv.Itoa(result.job.id) + ") - [" + result.objectPath + "] - Modified: " + result.lastModified.Format("2006-01-02 15:04:05") + " - To: " + result.contentType)
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

func copyWithinS3(s3bucket string, objectpath string, s3svc *s3.S3) (result *s3.CopyObjectResult, newtype string) {
	var filename string = path.Base(objectpath)
	// Set the Content-type based upon the file extension
	var contenttype string = mime.TypeByExtension(filepath.Ext(filename))

	input := &s3.CopyObjectInput{
		Bucket:      aws.String(s3bucket),
		Key:         aws.String(objectpath),
		CopySource:  aws.String(s3bucket + objectpath),
		ContentType: aws.String(contenttype),
		MetadataDirective: aws.String("REPLACE"),
	}

	//fmt.Println(input)

	output, err := s3svc.CopyObject(input)
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
		return
	}

	result = output.CopyObjectResult

	return result, contenttype
}
