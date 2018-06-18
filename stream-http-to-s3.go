package main

import (
	"bufio"
	"flag"
	"fmt"
	//"github.com/rlmcpherson/s3gof3r"
	// Using the mediapeers fork due to fixes around special characters
	//"github.com/mediapeers/s3gof3r"
	// Using the teamlakana fork due to fixes around special characters
	"github.com/teamlakana/s3gof3r"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

const (
	//MaxIdleConnections int = 10000
	MaxIdleConnections int = 1
	//RequestTimeout     int = 5
)

type Job struct {
	id          int
	baseHttpUrl string
	s3Bucket    string
	objectPath  string
}
type Result struct {
	job   Job
	bytes int64
	code  int
	s3Url string
}

var (
	jobs         = make(chan Job, 2000)
	results      = make(chan Result, 2000)
	concurrency  = flag.Int("c", 4, "The concurrent number of workers to start.")
	delay        = flag.Int("d", 1000, "The millisecond delay between starting workers.")
	baseHostname = flag.String("b", "", "The base hostname to fetch from.")
	s3Bucket     = flag.String("s", "", "The s3 bucket to stream into.")
	getUrlPrefix = flag.String("p", *s3Bucket, "The prefix to use on the request path. Defaults to S3 bucket name.")
	httpClient   *http.Client
)

func createHTTPClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: MaxIdleConnections,
		},
		//Timeout: time.Duration(RequestTimeout) * time.Second,
	}

	return client
}

func worker(wg *sync.WaitGroup, k s3gof3r.Keys) {
	httpClient = createHTTPClient()
	for job := range jobs {
		b, c, u := streamHttpToS3(job.baseHttpUrl, job.s3Bucket, job.objectPath, k)
		output := Result{job: job, bytes: b, code: c, s3Url: u}
		results <- output
	}
	wg.Done()
}
func createWorkerPool(noOfWorkers int) {
	var wg sync.WaitGroup
	// Read in the S3 access keys from environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
	keys, err := s3gof3r.EnvKeys()
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go worker(&wg, keys)
		// Ramp up the number of workers to slow the initial request rate down
		time.Sleep(time.Duration(*delay) * time.Millisecond)
	}
	wg.Wait()
	close(results)
}
func result(done chan bool) {
	for result := range results {
		log.Print("(id " + strconv.Itoa(result.job.id) + ") - code: " + strconv.Itoa(result.code) + " bytes: " + strconv.FormatInt(result.bytes, 10) + " - [" + result.s3Url + "]")
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
			jobs <- Job{id: counter, baseHttpUrl: "http://" + *baseHostname, s3Bucket: *s3Bucket, objectPath: line}
			counter++
		}
		close(jobs)
	}()

	done := make(chan bool)
	go result(done)
	createWorkerPool(*concurrency)
	<-done
	endTime := time.Now()
	diff := endTime.Sub(startTime)
	fmt.Println("total time taken ", diff.Seconds(), "seconds")
}

func streamHttpToS3(basehttpurl string, s3bucket string, objectpath string, keys s3gof3r.Keys) (int64, int, string) {
	objectpathAsUrl, err := url.Parse(objectpath)
	if err != nil {
		panic(err)
	}
	// objectpathAsUrl.String()
	var filename string = path.Base(objectpath)
	var geturl = basehttpurl + "/" + *getUrlPrefix + objectpathAsUrl.String()

	// Set the Content-type based upon the file extension
	var contenttype string = mime.TypeByExtension(filepath.Ext(filename))

	// Create a pipe
	pipeoutput, pipeinput := io.Pipe()

	// Open bucket we want to write a file to
	s3 := s3gof3r.New("", keys)
	bucket := s3.Bucket(s3bucket)

	header := make(http.Header)
	header.Add("Content-Type", contenttype)

	// open a PutWriter for S3 upload
	s3writer, err := bucket.PutWriter(objectpath, header, nil)
	if err != nil {
		log.Print("Failed S3 PUT: " + objectpath + " Error was: " + fmt.Sprint(err))
		//log.Fatal(err)
	}
	defer s3writer.Close()

	// Open HTTP GET for our object
	var statuscode int
	// Wrap in a function to prevent deadlock
	go func() {
		defer pipeinput.Close()
		//getresponse, err := http.Get(geturl)
		getresponse, err := httpClient.Get(geturl)
		if err != nil {
			log.Print("Failed HTTP GET: " + objectpath + " Error was: " + fmt.Sprint(err))
			//log.Fatal(err)
		}
		defer getresponse.Body.Close()
		_, err = io.Copy(pipeinput, getresponse.Body)
		if err != nil {
			log.Print("Failed io.Copy operation: " + objectpath + " Error was: " + fmt.Sprint(err))
		}
		statuscode = getresponse.StatusCode
	}()

	// Pass opened HTTP GET to S3
	var bytesxferred int64
	bytesxferred, err = io.Copy(s3writer, pipeoutput)
	if err != nil {
		log.Print("Failed io.Copy operation: " + objectpath + " Error was: " + fmt.Sprint(err))
		//log.Fatal(err)
	}
	return bytesxferred, statuscode, geturl
}
