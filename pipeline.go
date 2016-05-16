package connector

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/ezoic/go-kinesis"
	l4g "github.com/ezoic/log4go"
)

// Pipeline is used as a record processor to configure a pipline.
//
// The user should implement this such that each method returns a configured implementation of each
// interface. It has a data type (Model) as Records come in as a byte[] and are transformed to a Model.
// Then they are buffered in Model form and when the buffer is full, Models's are passed to the emitter.
type Pipeline struct {
	Buffer                    Buffer
	Checkpoint                Checkpoint
	Emitter                   Emitter
	Filter                    Filter
	StreamName                string
	Transformer               Transformer
	CheckpointFilteredRecords bool
}

// ProcessShard kicks off the process of a Kinesis Shard.
// It is a long running process that will continue to read from the shard.
func (p Pipeline) ProcessShard(ksis *kinesis.Kinesis, shardID string) {

	expiredIteratorCount := 0

	for true {

		err := p.processShardInternal(ksis, shardID, &expiredIteratorCount)
		if err == nil {
			p.Checkpoint.SetClosed(shardID, true)
			l4g.Info("stream %v, shard %v has been closed", p.StreamName, shardID)
			return
		} else if kerr, ok := err.(*kinesis.Error); ok && (kerr.Code == "ExpiredIteratorException" || kerr.Code == "ServiceUnavailable") {
			expiredIteratorCount++
			if expiredIteratorCount < 10 {
				l4g.Warn("expired iterator count %d: %v", expiredIteratorCount, kerr)
			} else {
				log.Fatalf("ProcessShard ERROR too many expired iterators: %v\n", err)
			}
		} else {
			log.Fatalf("ProcessShard ERROR: %#v (%v)\n", err, reflect.TypeOf(err).String())
		}
	}

}

func (p Pipeline) processShardInternal(ksis *kinesis.Kinesis, shardID string, expiredIteratorCount *int) error {

	args := kinesis.NewArgs()
	args.Add("ShardId", shardID)
	args.Add("StreamName", p.StreamName)

	if p.Checkpoint.CheckpointExists(shardID) {
		args.Add("ShardIteratorType", "AFTER_SEQUENCE_NUMBER")
		args.Add("StartingSequenceNumber", p.Checkpoint.SequenceNumber())
	} else {
		args.Add("ShardIteratorType", "TRIM_HORIZON")
	}

	shardInfo, err := ksis.GetShardIterator(args)

	if err != nil {
		return err
	}

	shardIterator := shardInfo.ShardIterator

	consecutiveErrorAttempts := 0

	for {

		if consecutiveErrorAttempts > 50 {
			log.Fatalf("Too many consecutive error attempts on shard %v", shardID)
		}

		// handle the aws backoff stuff
		HandleAwsWaitTimeExp(consecutiveErrorAttempts, "shard ID "+shardID)

		args = kinesis.NewArgs()
		args.Add("ShardIterator", shardIterator)
		startTime := time.Now()
		recordSet, err := ksis.GetRecords(args)
		getRecordsDuration := time.Now().Sub(startTime)
		if getRecordsDuration.Seconds() > 30 {
			l4g.Warn("kinesis request duration [%v] on shard [%v]", getRecordsDuration.String(), shardID)
		}

		if err != nil {
			if IsRecoverableError(err) {
				consecutiveErrorAttempts++
				if consecutiveErrorAttempts > 6 || strings.Contains(err.Error(), "ProvisionedThroughputExceededException") == false {
					l4g.Warn("recoverable error for shard [%v], %s (%d) type=%v", shardID, err, consecutiveErrorAttempts, reflect.TypeOf(err).String())
				}
				continue
			} else {
				return err
			}
		} else {
			consecutiveErrorAttempts = 0
			*expiredIteratorCount = 0
		}

		if len(recordSet.Records) > 0 {
			for _, v := range recordSet.Records {
				data := v.GetData()

				r := p.Transformer.ToRecord(data)

				if p.Filter.KeepRecord(r) {
					p.Buffer.ProcessRecord(r, v.SequenceNumber, int(v.ApproximateArrivalTimestamp))
				} else if p.CheckpointFilteredRecords {
					p.Buffer.ProcessRecord(nil, v.SequenceNumber, int(v.ApproximateArrivalTimestamp))
				}
			}
		} else if recordSet.NextShardIterator == "" {
			l4g.Debug("stream %v, shard %v has returned an empty NextShardIterator.  this indicates that it is closed.", p.StreamName, shardID)
			return nil
		} else if shardIterator == recordSet.NextShardIterator {
			return fmt.Errorf("NextShardIterator ERROR: %v", recordSet.NextShardIterator)
		} else {
			l4g.Fine("no records received, sleeping")
			time.Sleep(5 * time.Second)
		}

		if p.Buffer.ShouldFlush() {
			if p.Buffer.NumRecordsInBuffer() > 0 {
				err := p.Emitter.Emit(p.Buffer, p.Transformer, shardID)
				if err != nil {
					return err
				}
			}
			p.Checkpoint.SetCheckpoint(shardID, p.Buffer.LastSequenceNumber(), p.Buffer.LastApproximateArrivalTime())
			p.Buffer.Flush()
		}

		shardIterator = recordSet.NextShardIterator
	}

	return nil
}
