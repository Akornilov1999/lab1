#!/bin/bash
if [[ $# -eq 0 ]] ; then
    echo 'You should specify output file!'
    exit 1
fi

METRICID=("1" "2" "3")
NUMBER=10
CONSTNUMBER=1510670900000

rm -rf input
mkdir input

for i in {1..200}
	do
	  VAL=$(($RANDOM % 200))
	  NUMBER=$(($NUMBER + $(($RANDOM % 5000))))
	  RESULTNUMBER=$(($CONSTNUMBER+$NUMBER))
		RESULT="${METRICID[$((RANDOM % ${#METRICID[*]}))]}, $RESULTNUMBER, $VAL"
		echo $RESULT >> input/$1.1
	done