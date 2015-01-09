#!/bin/bash

echo -e "\nTerminating ELClassifier java process....\n"
pssh -p 500 -h hosts.txt -t 0 -o term-output -e term-error -I < scripts/term-el.sh
