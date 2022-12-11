#!/usr/bin/env bash

# Run the application
sbt "run --init"

# Run with fill 20M records
sbt "run --fill 20000000"