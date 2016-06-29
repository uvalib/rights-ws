#!/bin/bash
env GOOS=linux go build -o dist/rights-ws.linux
cp config.yml.template dist/
