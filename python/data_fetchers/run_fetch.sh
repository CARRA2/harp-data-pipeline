#!/usr/bin/env bash
source ../../config/config.aa
CONF=streams_carra2.yml
CONF=streams_test_current.yml
module load python3
python3 fetch_data_yearly.py -config $CONF
