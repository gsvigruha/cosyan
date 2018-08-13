#!/bin/sh -xue

COSYAN_CONF=conf nohup java -jar cosyan-all.jar > cosyan.stdout.log 2> cosyan.stderr.log & echo $! > cosyan.pid

