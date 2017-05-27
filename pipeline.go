package connector

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"errors"

	"github.com/ezoic/go-kinesis"
	"github.com/ezoic/klease"
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
	ShardIteratorInitType     string
	CheckpointFilteredRecords bool
	GetRecordsLimit           int
	LeaseCoordinator          *klease.Coordinator
	RunningPipes              map[string]bool
}

// ProcessShard kicks off the process of a Kinesis Shard.
// It is a long running process that will continue to read from the shard.
func (p Pipeline) ProcessShard(ksis *kinesis.Kinesis, shardID string) {
	expiredIteratorCount := 0

	for true {

		err := p.processShardInternal(ksis, shardID, &expiredIteratorCount)
		if err == nil {
			p.Checkpoint.SetClosed(shardID, true)
			l4g.Info("stream %s, shard %s has been closed", p.StreamName, shardID)
			if p.LeaseCoordinator != nil {
				err = p.LeaseCoordinator.DeleteLease(shardID)
				if err != nil {
					l4g.Error("stream %s, shard %s has been closed but lease could not be deleted", p.StreamName, shardID)
				}
			}
			return
		} else if kerr, ok := err.(*kinesis.Error); ok && (kerr.Code == "ExpiredIteratorException" || kerr.Code == "ServiceUnavailable" || strings.Contains(kerr.Message, "temporary failure of the server")) {
			expiredIteratorCount++
			if expiredIteratorCount < 10 {
				l4g.Warn("expired iterator count %d: %v", expiredIteratorCount, kerr)
			} else {
				log.Fatalf("ProcessShard ERROR too many expired iterators: %v\n", err)
			}
		} else if err.Error() == "LostOwnership" {
			l4g.Info("\n\n\nstream %s, shard %s has changed owners\n\n\n", p.StreamName, shardID)
			//let kauto know we are off so we have the ability to start this shard again if we ever regain ownership
			if p.RunningPipes != nil {
				p.RunningPipes[shardID] = false
			}
			return
		} else {
			//let l4g have time to flush before we kill everything
			time.Sleep(100 * time.Millisecond)
			log.Fatalf("ProcessShard ERROR: on shard %s and stream %s %#v (%v)\n%v\n", shardID, p.StreamName, err, reflect.TypeOf(err).String(), err.Error())
		}
	}

}

func (p Pipeline) processShardInternal(ksis *kinesis.Kinesis, shardID string, expiredIteratorCount *int) error {

	args := kinesis.NewArgs()
	args.Add("ShardId", shardID)
	args.Add("StreamName", p.StreamName)

	if p.Checkpoint.CheckpointExists(shardID) {
		if p.Checkpoint.CheckpointIsClosed(shardID) {
			return nil
		}
		args.Add("ShardIteratorType", "AFTER_SEQUENCE_NUMBER")
		args.Add("StartingSequenceNumber", p.Checkpoint.SequenceNumber())
	} else if len(p.ShardIteratorInitType) != 0 {
		args.Add("ShardIteratorType", p.ShardIteratorInitType)
	} else {
		args.Add("ShardIteratorType", "TRIM_HORIZON")
	}

	shardInfo, err := ksis.GetShardIterator(args)

	if err != nil {
		return err
	}

	shardIterator := shardInfo.ShardIterator

	consecutiveErrorAttempts := 0
	//provisionedThroughputExceededCount := 0

	for {

		if consecutiveErrorAttempts > 50 {
			log.Fatalf("Too many consecutive error attempts on shard %v", shardID)
		}

		// handle the aws backoff stuff
		HandleAwsWaitTimeExp(consecutiveErrorAttempts, "shard ID "+shardID)

		args = kinesis.NewArgs()
		args.Add("ShardIterator", shardIterator)
		if p.GetRecordsLimit > 0 {
			args.Add("Limit", p.GetRecordsLimit)
		}
		startTime := time.Now()
		recordSet, err := ksis.GetRecords(args)
		getRecordsDuration := time.Now().Sub(startTime)
		if getRecordsDuration.Seconds() > 30 {
			l4g.Warn("kinesis request duration [%s] on stream [%s] shard [%s]", getRecordsDuration.String(), p.StreamName, shardID)
		}

		if err != nil {
			if IsRecoverableError(err) {
				consecutiveErrorAttempts++

				// Throttle by the number of times that provisionedThroughputExceeded is seen
				//				if strings.Contains(err.Error(), "ProvisionedThroughputExceededException") == true {
				//					provisionedThroughputExceededCount++
				//					time.Sleep(time.Millisecond * time.Duration(200*provisionedThroughputExceededCount))
				//				}
				if consecutiveErrorAttempts > 6 || strings.Contains(err.Error(), "ProvisionedThroughputExceededException") == false {
					l4g.Warn("recoverable error for stream [%s] shard [%s], %s (%d) type=%s", p.StreamName, shardID, err, consecutiveErrorAttempts, reflect.TypeOf(err).String())
				}
				continue
			} else {
				return err
			}
		} else {
			consecutiveErrorAttempts = 0
			*expiredIteratorCount = 0
			//provisionedThroughputExceededCount = 0
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
			l4g.Debug("stream %s, shard %s has returned an empty NextShardIterator.  this indicates that it is closed.", p.StreamName, shardID)
			err = p.flushBuffer(shardID)
			return err
		} else if shardIterator == recordSet.NextShardIterator {
			return fmt.Errorf("NextShardIterator ERROR: %v", recordSet.NextShardIterator)
		} else if err == nil && recordSet.MillisBehindLatest < 10000 {
			l4g.Fine("no records received, sleeping")
			time.Sleep(5 * time.Second)
		}

		if p.Buffer.ShouldFlush() {
			if err = p.flushBuffer(shardID); err != nil {
				return err
			}
		}

		shardIterator = recordSet.NextShardIterator

		// Should only call getRecords on kinesis 5 times per second per shard
		// This is here to throttle incase we are pulling too fast
		//time.Sleep(time.Millisecond * 200)
	}

	return nil
}

func (p Pipeline) flushBuffer(shardID string) error {
	//we lost ownership. stop working.
	if p.LeaseCoordinator != nil && p.LeaseCoordinator.GetCurrentlyHeldLease(shardID) == nil {
		return errors.New("LostOwnership")
	}

	if p.Buffer.NumRecordsInBuffer() > 0 {
		err := p.Emitter.Emit(p.Buffer, p.Transformer, shardID)
		if err != nil {
			return err
		}
	}
	p.Checkpoint.SetCheckpoint(shardID, p.Buffer.LastSequenceNumber(), p.Buffer.LastApproximateArrivalTime())
	p.Buffer.Flush()

	return nil
}
