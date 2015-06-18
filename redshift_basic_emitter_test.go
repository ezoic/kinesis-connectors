package connector

import (
	"database/sql"
	"os"
	"testing"

	_ "github.com/lib/pq"
)

func TestCopyStatement(t *testing.T) {
	e := RedshiftBasicEmtitter{
		Delimiter: ",",
		S3Bucket:  "test_bucket",
		TableName: "test_table",
	}
	f := e.copyStatement("test.txt")

	copyStatement := "COPY test_table FROM 's3://test_bucket/test.txt' CREDENTIALS 'aws_access_key_id=;aws_secret_access_key=' DELIMITER ',';"

	if f != copyStatement {
		t.Errorf("copyStatement() = %s want %s", f, copyStatement)
	}
}

func Test_WriteInvalidDataToTable(t *testing.T) {

	db, err := sql.Open("postgres", os.Getenv("REDSHIFT_URL"))
	if err != nil {
		t.Fatal(err)
	}

	// this test is determining whether writing invalid data to a redshift table
	// is detected as an invalid load
	emitter := RedshiftBasicEmtitter{
		TableName: "test.testtable",
		Format:    "json",
		Delimiter: ",",
		S3Bucket:  os.Getenv("REDSHIFT_S3_BUCKET"),
		S3Prefix:  os.Getenv("REDSHIFT_S3_PREFIX"),
		Db:        db,
	}
	transformer := StringToStringTransformer{}
	buffer := &RecordBuffer{NumRecordsToBuffer: 1}
	buffer.ProcessRecord("{\"id\":1234,\"thiscoldoesnotexist\":789}", "11111111111111")

	err = emitter.Emit(buffer, transformer)
	if err == nil {
		t.Fatal("expected an error from invalid input")
	}

}

func Test_WriteValidDataToTable(t *testing.T) {

	db, err := sql.Open("postgres", os.Getenv("REDSHIFT_URL"))
	if err != nil {
		t.Fatal(err)
	}

	// this test is determining whether writing invalid data to a redshift table
	// is detected as an invalid load
	emitter := RedshiftBasicEmtitter{
		TableName: "test.testtable",
		Format:    "json",
		Delimiter: ",",
		S3Bucket:  os.Getenv("REDSHIFT_S3_BUCKET"),
		S3Prefix:  os.Getenv("REDSHIFT_S3_PREFIX"),
		Db:        db,
	}
	transformer := StringToStringTransformer{}
	buffer := &RecordBuffer{NumRecordsToBuffer: 1}
	buffer.ProcessRecord("{\"id\":1234,\"value\":\"danisawesome\"}", "11111111111111")

	err = emitter.Emit(buffer, transformer)
	if err != nil {
		t.Fatalf("got an error from valid input, %s", err)
	}

}
