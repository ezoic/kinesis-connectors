package connector

import "time"

// RecordBuffer is a basic implementation of the Buffer interface.
// It buffer's records and answers questions on when it should be periodically flushed.
type RecordBuffer struct {
	NumRecordsToBuffer         int
	MaxTimeBetweenFlush        time.Duration
	lastApproximateArrivalTime int

	lastFlush           time.Time
	firstSequenceNumber string
	lastSequenceNumber  string
	recordsInBuffer     []interface{}
	sequencesInBuffer   []string
}

// ProcessRecord adds a message to the buffer.
func (b *RecordBuffer) ProcessRecord(record interface{}, sequenceNumber string, approximateArrivalTime int) {
	if b.lastFlush.IsZero() {
		b.lastFlush = time.Now()
	}

	if len(b.sequencesInBuffer) == 0 {
		b.firstSequenceNumber = sequenceNumber
	}

	b.lastSequenceNumber = sequenceNumber
	b.lastApproximateArrivalTime = approximateArrivalTime

	if !b.sequenceExists(sequenceNumber) {
		if record != nil {
			b.recordsInBuffer = append(b.recordsInBuffer, record)
		}
		b.sequencesInBuffer = append(b.sequencesInBuffer, sequenceNumber)
	}
}

// Records returns the records in the buffer.
func (b *RecordBuffer) Records() []interface{} {
	return b.recordsInBuffer
}

// NumRecordsInBuffer returns the number of messages in the buffer.
func (b RecordBuffer) NumRecordsInBuffer() int {
	return len(b.recordsInBuffer)
}

// Flush empties the buffer and resets the sequence counter.
func (b *RecordBuffer) Flush() {
	b.lastFlush = time.Now()
	b.recordsInBuffer = b.recordsInBuffer[:0]
	b.sequencesInBuffer = b.sequencesInBuffer[:0]
}

// Checks if the sequence already exists in the buffer.
func (b *RecordBuffer) sequenceExists(sequenceNumber string) bool {
	for _, v := range b.sequencesInBuffer {
		if v == sequenceNumber {
			return true
		}
	}
	return false
}

// ShouldFlush determines if the buffer has reached its target size.
func (b *RecordBuffer) ShouldFlush() bool {
	if len(b.sequencesInBuffer) >= b.NumRecordsToBuffer {
		return true
	}

	if b.MaxTimeBetweenFlush > 0 && len(b.sequencesInBuffer) > 0 && time.Since(b.lastFlush) > b.MaxTimeBetweenFlush {
		return true
	}
	return false
}

// FirstSequenceNumber returns the sequence number of the first message in the buffer.
func (b *RecordBuffer) FirstSequenceNumber() string {
	return b.firstSequenceNumber
}

// LastSequenceNumber returns the sequence number of the last message in the buffer.
func (b *RecordBuffer) LastSequenceNumber() string {
	return b.lastSequenceNumber
}

func (b *RecordBuffer) LastApproximateArrivalTime() int {
	return b.lastApproximateArrivalTime
}
