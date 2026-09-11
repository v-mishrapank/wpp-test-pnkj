#!/bin/bash

./config.sh \
  --url $REPO_URL \
  --token $RUNNER_TOKEN \
  --unattended \
  --replace

cleanup() {
  ./config.sh remove --token $RUNNER_TOKEN
}

trap cleanup EXIT

./run.sh