package connector

import (
	"github.com/ezoic/go-kinesis"
	l4g "github.com/ezoic/log4go"
)

// An implementation of Emitter that puts event data on S3 file, and then puts the
// S3 file path onto the output stream for processing by manifest application.
type S3ManifestEmitter struct {
	OutputStream string
	S3Bucket     string
	Ksis         *kinesis.Kinesis
}

func (e S3ManifestEmitter) Emit(b Buffer, t Transformer, shardID string) error {

	// Emit buffer contents to S3 Bucket
	s3Emitter := S3Emitter{S3Bucket: e.S3Bucket}
	s3Emitter.Emit(b, t, shardID)
	s3File := s3Emitter.S3FileName(b.FirstSequenceNumber(), b.LastSequenceNumber())

	// Emit the file path to Kinesis Output stream
	args := kinesis.NewArgs()
	args.Add("StreamName", e.OutputStream)
	args.Add("PartitionKey", s3File)
	args.AddData([]byte(s3File))

	_, err := e.Ksis.PutRecord(args)

	if err != nil {
		l4g.Error("PutRecord ERROR: %v", err)
		return err
	} else {
		l4g.Info("[%s] emitted to [%s] on shard [%v]", b.FirstSequenceNumber(), e.OutputStream, shardID)
	}
	return nil
}
