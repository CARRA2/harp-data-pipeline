#!/usr/bin/env bash
source ../../config/config.aa
SCR=fetch_simple.go
module load go
if [ ! -f ./$SCR ]; then
go build $SCR
./$SCR --config streams_test.yml
else
#go run fetch_data_carra2.go --config streams_test.yml
./$SCR --config streams_test.yml
fi
