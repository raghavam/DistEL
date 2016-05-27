#!/bin/bash

# used '[]' to avoid getting this as grep result
kill $(ps aux | grep '[re]dis' | awk '{print $2}')
