#!/bin/bash

cd redis-3.2.0/
src/redis-server redis.conf > redis-log &
