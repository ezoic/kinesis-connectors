module github.com/ezoic/kinesis-connectors

go 1.18

replace (
	github.com/ezoic/data-model => ../data-model
	github.com/ezoic/ezoicdb => ../ezoicdb
	github.com/ezoic/ezutil => ../ezutil
	github.com/ezoic/go-charset => ../go-charset
	github.com/ezoic/go-diff => ../go-diff
	github.com/ezoic/go-kinesis => ../go-kinesis
	github.com/ezoic/klease => ../klease
	github.com/ezoic/log4go => ../log4go
	github.com/ezoic/sheriff => ../sheriff
	github.com/ezoic/subscriptions => ../subscriptions
	github.com/ezoic/uasurfer => ../uasurfer
)

require (
	github.com/AdRoll/goamz v0.0.0-20170825154802-2731d20f46f4
	github.com/ezoic/go-kinesis v0.0.0
	github.com/ezoic/klease v0.0.0
	github.com/ezoic/log4go v0.0.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/jackc/pgx v3.6.2+incompatible
	github.com/shopspring/decimal v1.3.1
)

require (
	github.com/aws/aws-sdk-go v1.42.46 // indirect
	github.com/cockroachdb/apd v1.1.0 // indirect
	github.com/gofrs/uuid v4.2.0+incompatible // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/lib/pq v1.10.6 // indirect
	github.com/nu7hatch/gouuid v0.0.0-20131221200532-179d4d0c4d8d // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519 // indirect
	golang.org/x/text v0.3.6 // indirect
)
