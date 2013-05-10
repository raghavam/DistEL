#!/bin/bash

echo -e "\nTerminating ELClassifier process on this machine....\n"
kill $(ps aux | grep '[E]L' | awk '{print $2}')
