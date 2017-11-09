package connector

import (
	"bytes"
	"fmt"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/s3"
	l4g "github.com/ezoic/log4go"
)

// S3Emitter is an implementation of Emitter used to store files from a Kinesis stream in S3.
//
// The use of  this struct requires the configuration of an S3 bucket/endpoint. When the buffer is full, this
// struct's Emit method adds the contents of the buffer to S3 as one file. The filename is generated
// from the first and last sequence numbers of the records contained in that file separated by a
// dash. This struct requires the configuration of an S3 bucket and endpoint.
type S3Emitter struct {
	S3Bucket string
	S3Prefix string
}

// S3FileName generates a file name based on the First and Last sequence numbers from the buffer. The current
// UTC date (YYYY-MM-DD) is base of the path to logically group days of batches.
func (e S3Emitter) S3FileName(firstSeq string, lastSeq string) string {
	date := time.Now().UTC().Format("2006/01/02")
	if e.S3Prefix == "" {
		return fmt.Sprintf("%v/%v-%v", date, firstSeq, lastSeq)
	} else {
		return fmt.Sprintf("%v/%v/%v-%v", e.S3Prefix, date, firstSeq, lastSeq)
	}
}

// Emit is invoked when the buffer is full. This method emits the set of filtered records.
func (e S3Emitter) Emit(b Buffer, t Transformer, shardID string) error {
	auth, _ := aws.EnvAuth()
	s3Con := s3.New(auth, aws.USEast)
	s3Con.ReadTimeout = time.Second * 300
	s3Con.ConnectTimeout = time.Second * 10
	bucket := s3Con.Bucket(e.S3Bucket)
	s3File := e.S3FileName(b.FirstSequenceNumber(), b.LastSequenceNumber())

	var buffer bytes.Buffer

	for _, r := range b.Records() {
		var s = t.FromRecord(r)
		buffer.Write(s)
	}

	var err error
	for i := 0; i < 10; i++ {

		// handle aws backoff, this may be necessary if, for example, the
		// s3 file has not appeared to the database yet
		HandleAwsWaitTimeExp(i, "s3 emitter on shard "+shardID)

		err = bucket.Put(s3File, buffer.Bytes(), "text/plain", s3.Private, s3.Options{})

		if err == nil || IsRecoverableError(err) == false {
			l4g.Fine("exiting loop")
			break
		}

		// recoverable error, lets warn
		l4g.Warn("recoverable s3 error %v on shard [%v]", err, shardID)

	}

	if err != nil {
		l4g.Error("S3Put ERROR: %v", err)
		return err
	} else {
		l4g.Debug("[%v] records emitted to [s3://%v/%v] on shard [%v]", b.NumRecordsInBuffer(), e.S3Bucket, s3File, shardID)
	}
	return nil
}
