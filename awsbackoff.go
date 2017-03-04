package connector

import (
	"io"
	"math"
	"math/rand"
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
)

type RecoverableErrorTestFunc func(error) bool
type RecoverableErrorTester interface {
	IsRecoverableError(error) bool
}
type recoverableErrorTesterType struct {
	f RecoverableErrorTestFunc
}

func (i *recoverableErrorTesterType) IsRecoverableError(err error) bool {
	return i.f(err)
}
func NewRecoverableErrorTester(f RecoverableErrorTestFunc) RecoverableErrorTester {
	return &recoverableErrorTesterType{f: f}
}

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

func textIsRecoverableError(err error) bool {
	recoverableErrors := []string{
		"Client.Timeout exceeded while reading body",
		"was not read from or written to within the timeout period",
		"Deadlock",
	}
	for _, txt := range recoverableErrors {
		if strings.Contains(err.Error(), txt) {
			return true
		}
	}
	return false
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
	for _, re := range redshiftRecoverableErrors {
		if re.MatchString(err.Error()) {
			r = true
			break
		}
	}
	return r
}

func s3IsRecoverableError(err error) bool {
	recoverableErrorCodes := map[string]bool{
		"ProvisionedThroughputExceededException": true,
		"InternalError":                          true,
		"InternalFailure":                        true,
		"Throttling":                             true,
		"ServiceUnavailable":                     true,
		"SlowDown":                               true,
		//"ExpiredIteratorException":               true,
	}
	r := false
	cErr, ok := err.(*s3.Error)
	if ok && (recoverableErrorCodes[cErr.Code] == true || cErr.StatusCode == http.StatusInternalServerError) {
		r = true
	}
	return r
}

func sqlIsRecoverableError(err error) bool {
	r := false

	if strings.Contains(err.Error(), "current transaction is aborted") {
		r = true
	}

	return r
}

var recoverableErrorTesters = map[string]RecoverableErrorTester{
	"kinesis":  NewRecoverableErrorTester(kinesisIsRecoverableError),
	"network":  NewRecoverableErrorTester(netIsRecoverableError),
	"url":      NewRecoverableErrorTester(urlIsRecoverableError),
	"redshift": NewRecoverableErrorTester(redshiftIsRecoverableError),
	"s3":       NewRecoverableErrorTester(s3IsRecoverableError),
	"sql":      NewRecoverableErrorTester(sqlIsRecoverableError),
	"text":     NewRecoverableErrorTester(textIsRecoverableError),
}

// this determines whether the error is recoverable
func IsRecoverableError(err error) bool {
	r := false

	l4g.Debug("isRecoverableError, type %s, value (%#v)\n", reflect.TypeOf(err).String(), err)

	for _, t := range recoverableErrorTesters {
		r = t.IsRecoverableError(err)
		if r {
			break
		}
	}

	return r
}

func AddRecoverableErrorTester(name string, i RecoverableErrorTester) {
	recoverableErrorTesters[name] = i
}

// handle the aws exponential backoff
func HandleAwsWaitTimeExp(attempts int, infoString string) {

	//http://docs.aws.amazon.com/general/latest/gr/api-retries.html
	// wait up to 5 minutes based on the aws exponential backoff algorithm
	if attempts > 0 {
		// jitter: https://www.awsarchitectureblog.com/2015/03/backoff.html
		// this is the full jitter.
		waitTime := time.Duration(rand.Intn(int(math.Min(100*math.Pow(2, float64(attempts)), 300000)))) * time.Millisecond
		if attempts > 6 {
			l4g.Info("aws error attempt %v failed for %s, waiting %s", attempts, infoString, waitTime.String())
		}
		time.Sleep(waitTime)
	}

}
