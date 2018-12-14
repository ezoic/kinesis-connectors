package connector

import (
	"database/sql"
	"fmt"
	"time"

	l4g "github.com/ezoic/log4go"
	_ "github.com/go-sql-driver/mysql"
)

// MysqlCheckpoint implements the Checkpont interface.
// This class is used to enable the Pipeline.ProcessShard to checkpoint their progress.
type MysqlCheckpoint struct {
	AppName    string
	StreamName string
	TableName  string
	Db         *sql.DB
	ServerId   string

	sequenceNumber string
	isClosed       bool
}

// CheckpointExists determines if a checkpoint for a particular Shard exists.
// Typically used to determine whether we should start processing the shard with
// TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *MysqlCheckpoint) CheckpointExists(shardID string) bool {

	l4g.Finest("SELECT sequence_number, is_closed FROM " + c.TableName + " WHERE checkpoint_key = ?")

	row := c.Db.QueryRow("SELECT sequence_number, is_closed FROM "+c.TableName+" WHERE checkpoint_key = ?", c.key(shardID))
	var val string
	var isClosed sql.NullInt64
	err := row.Scan(&val, &isClosed)
	if err == nil {
		l4g.Finest("sequence:%s", val)
		c.sequenceNumber = val
		if isClosed.Valid == false {
			c.isClosed = true
		} else {
			c.isClosed = (isClosed.Int64 != 0)
		}
		return true
	}

	if err == sql.ErrNoRows {
		c.isClosed = false
		return false
	}

	// something bad happened, better blow up the process
	panic(err)
}

// CheckpointIsClosed determines if a checkpoint for a particular Shard exists and is closed already
// Typically used to determine whether we should start processing the shard with
// TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *MysqlCheckpoint) CheckpointIsClosed(shardID string) bool {
	return c.isClosed
}

// CheckpointExists determines if a checkpoint for a particular Shard exists.
// Typically used to determine whether we should start processing the shard with
// TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *MysqlCheckpoint) DeleteCheckpoint(shardID string) bool {

	l4g.Finest("DELETE FROM " + c.TableName + " WHERE checkpoint_key = ?")

	_, err := c.Db.Exec("DELETE FROM "+c.TableName+" WHERE checkpoint_key = ?", c.key(shardID))
	if err == nil {
		return true
	}

	// something bad happened, better blow up the process
	panic(err)
}

// SequenceNumber returns the current checkpoint stored for the specified shard.
func (c *MysqlCheckpoint) SequenceNumber() string {
	return c.sequenceNumber
}

func (c *MysqlCheckpoint) SetClosed(shardID string, isClosed bool) {
	var isClosedInt int
	if isClosed {
		isClosedInt = 1
	} else {
		isClosedInt = 0
	}

	dtString := time.Now().Format("2006-01-02 15:04:05-0700")
	approximateArrivalTime := 0
	_, err := c.Db.Exec("INSERT INTO "+c.TableName+" (sequence_number, checkpoint_key, last_updated, last_arrival_time, server_id, is_closed) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE last_updated = VALUES(last_updated), server_id = VALUES(server_id), is_closed = VALUES(is_closed)", c.sequenceNumber, c.key(shardID), dtString, approximateArrivalTime, c.ServerId, isClosedInt)
	if err != nil {
		panic(err)
	}
}

// SetCheckpoint stores a checkpoint for a shard (e.g. sequence number of last record processed by application).
// Upon failover, record processing is resumed from this point.
func (c *MysqlCheckpoint) SetCheckpoint(shardID string, sequenceNumber string, approximateArrivalTime int) {

	dtString := time.Now().Format("2006-01-02 15:04:05")

	_, err := MysqlRetryExec(c.Db, "INSERT INTO "+c.TableName+" (sequence_number, checkpoint_key, last_updated, last_arrival_time, server_id, is_closed) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE sequence_number = VALUES(sequence_number), last_updated = VALUES(last_updated), last_arrival_time = VALUES(last_arrival_time), server_id = VALUES(server_id), is_closed = VALUES(is_closed)", sequenceNumber, c.key(shardID), dtString, approximateArrivalTime, c.ServerId, 0)
	if err != nil {
		panic(err)
	}
	c.sequenceNumber = sequenceNumber

}

// key generates a unique mysql key for storage of Checkpoint.
func (c *MysqlCheckpoint) key(shardID string) string {
	return fmt.Sprintf("%v:checkpoint:%v:%v", c.AppName, c.StreamName, shardID)
}

func MysqlRetryExec(db *sql.DB, queryStatement string, args ...interface{}) (sql.Result, error) {

	i := 0
	var err error
	var affected sql.Result
	for true {

		HandleAwsWaitTimeExp(i, fmt.Sprintf("Retryable database error: %s", err.Error()))

		affected, err = db.Exec(queryStatement, args...)
		if err == nil {
			break
		}

		if IsRecoverableError(err) || i >= 10 {
			break
		}
		i++
	}

	return affected, err

}
