#!/bin/bash

kill $(ps aux | grep '[re]dis' | awk '{print $2}')
