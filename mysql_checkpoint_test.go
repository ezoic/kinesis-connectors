package connector

import (
	"database/sql"
	"os"
	"testing"
	"time"
)

func Test_MysqlKey(t *testing.T) {
	k := "app:checkpoint:stream:shard"
	c := MysqlCheckpoint{AppName: "app", StreamName: "stream"}

	r := c.key("shard")

	if r != k {
		t.Errorf("key() = %v, want %v", k, r)
	}
}

func Test_MysqlCheckpointExists(t *testing.T) {
	rc, _ := sql.Open("mysql", os.Getenv("CHECKPOINT_MYSQL_DSN"))
	k := "app:checkpoint:stream:shard"

	_, err := rc.Exec("INSERT INTO KinesisConnector.TestCheckpoint (sequence_number, checkpoint_key) VALUES (?, ?) ON DUPLICATE KEY UPDATE sequence_number = ?", "fakeSeqNum", k, "fakeSeqNum")
	if err != nil {
		t.Fatalf("cannot insert checkpoint into db manually, %s", err)
	}
	c := MysqlCheckpoint{AppName: "app", StreamName: "stream", TableName: "KinesisConnector.TestCheckpoint", Db: rc}

	r := c.CheckpointExists("shard")

	if r != true {
		t.Errorf("CheckpointExists() = %v, want %v", false, r)
	}

	rc.Exec("DELETE FROM KinesisConnector.TestCheckpoint WHERE checkpoint_key = ?", k)
}

func Test_MysqlSetCheckpoint(t *testing.T) {
	k := "app:checkpoint:stream:shard"

	rc, _ := sql.Open("mysql", os.Getenv("CHECKPOINT_MYSQL_DSN"))

	approximateArrivalTime := int(time.Now().Unix())

	c := MysqlCheckpoint{AppName: "app", StreamName: "stream", TableName: "KinesisConnector.TestCheckpoint", Db: rc}
	c.SetCheckpoint("shard", "fakeSeqNum", approximateArrivalTime)

	rslt := rc.QueryRow("SELECT sequence_number, last_updated, last_arrival_time FROM KinesisConnector.TestCheckpoint WHERE checkpoint_key = ?", k)
	var sequenceNumber, lastUpdated string
	var lastArrivalTime int
	err := rslt.Scan(&sequenceNumber, &lastUpdated, &lastArrivalTime)
	if err != nil {
		t.Fatalf("cannot scan row for checkpoint key, %s", err)
	}

	if sequenceNumber != "fakeSeqNum" {
		t.Errorf("SetCheckpoint() = %v, want %v", "fakeSeqNum", sequenceNumber)
	}
	if lastUpdated == "" {
		t.Error("last_updated is empty")
	}
	if lastArrivalTime != approximateArrivalTime {
		t.Errorf("last_arrival_time expected %v, actual %v", approximateArrivalTime, lastArrivalTime)
	}

	rc.Exec("DELETE FROM KinesisConnector.TestCheckpoint WHERE checkpoint_key = ?", k)
}
