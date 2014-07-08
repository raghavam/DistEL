#!/bin/bash

echo -e "\nClassifier starts....\n"
pssh -h hosts.txt -t 0 -o output -e error -I < scripts/classifier.sh

