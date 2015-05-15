package connector

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"

	// Postgres package is used when sql.Open is called
	l4g "github.com/ezoic/log4go"
	_ "github.com/lib/pq"
)

// RedshiftEmitter is an implementation of Emitter that buffered batches of records into Redshift one by one.
// It first emits records into S3 and then perfors the Redshift JSON COPY command. S3 storage of buffered
// data achieved using the S3Emitter. A link to jsonpaths must be provided when configuring the struct.
type RedshiftBasicEmitter struct {
	Delimiter string
	Format    string
	Jsonpaths string
	Logger    Logger
	S3Bucket  string
	S3Prefix  string
	TableName string
	Db        *sql.DB
}

// Emit is invoked when the buffer is full. This method leverages the S3Emitter and
// then issues a copy command to Redshift data store.
func (e RedshiftBasicEmitter) Emit(b Buffer, t Transformer) {
	s3Emitter := S3Emitter{S3Bucket: e.S3Bucket}
	s3Emitter.Emit(b, t)
	s3File := s3Emitter.S3FileName(b.FirstSequenceNumber(), b.LastSequenceNumber())

	stmt := e.copyStatement(s3File)

	var err error
	for i := 0; i < 10; i++ {

		// handle aws backoff, this may be necessary if, for example, the
		// s3 file has not appeared to the database yet
		handleAwsWaitTimeExp(i)

		// load into the database
		_, err := e.Db.Exec(stmt)

		// if the request succeeded, or its an unrecoverable error, break out of the loop
		// because we are done
		if err == nil || isRecoverableError(err) == false {
			break
		}

		// recoverable error, lets warn
		l4g.Warn(err)

	}

	if err != nil {
		e.Logger.Fatalf("db.Exec ERROR: %v\n", err)
	}

	e.Logger.Printf("Redshift load completed.\n")
}

// Creates the SQL copy statement issued to Redshift cluster.
func (e RedshiftBasicEmitter) copyStatement(s3File string) string {
	b := new(bytes.Buffer)
	b.WriteString(fmt.Sprintf("COPY %v ", e.TableName))
	b.WriteString(fmt.Sprintf("FROM 's3://%v/%v' ", e.S3Bucket, s3File))
	b.WriteString(fmt.Sprintf("CREDENTIALS 'aws_access_key_id=%v;", os.Getenv("AWS_ACCESS_KEY")))
	b.WriteString(fmt.Sprintf("aws_secret_access_key=%v' ", os.Getenv("AWS_SECRET_KEY")))
	switch e.Format {
	case "json":
		b.WriteString(fmt.Sprintf("json 'auto'"))
	case "jsonpaths":
		b.WriteString(fmt.Sprintf("json '%v'", e.Jsonpaths))
	default:
		b.WriteString(fmt.Sprintf("DELIMITER '%v'", e.Delimiter))
	}
	b.WriteString(";")
	l4g.Debug(b.String())
	return b.String()
}
