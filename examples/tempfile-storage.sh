#! /bin/bash

DST=$(mktemp)
cp $1 $DST
echo $DST
