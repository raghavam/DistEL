#!/bin/bash

echo -e "\nTerminating ELClassifier java process....\n"
pssh -h hosts.txt -t 0 -o term-output -e term-error -I < term-el.sh
