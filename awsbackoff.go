package connector

import (
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/AdRoll/goamz/s3"
	"github.com/ezoic/go-kinesis"
	l4g "github.com/ezoic/log4go"
	"github.com/lib/pq"
)

type isRecoverableErrorFunc func(error) bool

func kinesisIsRecoverableError(err error) bool {
	recoverableErrorCodes := map[string]bool{
		"ProvisionedThroughputExceededException": true,
		"InternalFailure":                        true,
		"Throttling":                             true,
		"ServiceUnavailable":                     true,
		//"ExpiredIteratorException":               true,
	}
	r := false
	cErr, ok := err.(*kinesis.Error)
	if ok && (recoverableErrorCodes[cErr.Code] == true || cErr.StatusCode == http.StatusInternalServerError) {
		r = true
	}
	return r
}

func urlIsRecoverableError(err error) bool {
	r := false
	_, ok := err.(*url.Error)
	if ok {
		r = true
	}
	return r
}

func netIsRecoverableError(err error) bool {
	recoverableErrors := map[string]bool{
		"connection reset by peer": true,
	}
	r := false

	if err == io.EOF {
		r = true
	} else if strings.Contains(err.Error(), "connection reset by peer") {
		r = true
	} else {
		cErr, ok := err.(*net.OpError)
		if ok && recoverableErrors[cErr.Err.Error()] == true {
			r = true
		}
	}
	return r
}

var redshiftRecoverableErrors = []*regexp.Regexp{
	regexp.MustCompile("The specified S3 prefix '.*?' does not exist"),
}

func redshiftIsRecoverableError(err error) bool {
	r := false
	if cErr, ok := err.(pq.Error); ok {
		for _, re := range redshiftRecoverableErrors {
			if re.MatchString(cErr.Message) {
				r = true
				break
			}
		}
	}
	return r
}

func s3IsRecoverableError(err error) bool {
	recoverableErrorCodes := map[string]bool{
		"ProvisionedThroughputExceededException": true,
		"InternalFailure":                        true,
		"Throttling":                             true,
		"ServiceUnavailable":                     true,
		//"ExpiredIteratorException":               true,
	}
	r := false
	cErr, ok := err.(*s3.Error)
	if ok && (recoverableErrorCodes[cErr.Code] == true || cErr.StatusCode == http.StatusInternalServerError) {
		r = true
	}
	return r
}

var isRecoverableErrors = []isRecoverableErrorFunc{
	kinesisIsRecoverableError, netIsRecoverableError, urlIsRecoverableError, redshiftIsRecoverableError, s3IsRecoverableError,
}

// this determines whether the error is recoverable
func IsRecoverableError(err error) bool {
	r := false

	l4g.Debug("isRecoverableError, type %s, value (%#v)\n", reflect.TypeOf(err).String(), err)

	for _, errF := range isRecoverableErrors {
		r = errF(err)
		if r {
			break
		}
	}

	return r
}

// handle the aws exponential backoff
func HandleAwsWaitTimeExp(attempts int) {

	//http://docs.aws.amazon.com/general/latest/gr/api-retries.html
	// wait up to 5 minutes based on the aws exponential backoff algorithm
	if attempts > 0 {
		waitTime := time.Duration(math.Min(100*math.Pow(2, float64(attempts)), 300000)) * time.Millisecond
		l4g.Finest("handleAwsWaitTimeExp:%s", waitTime.String())
		time.Sleep(waitTime)
	}

}
