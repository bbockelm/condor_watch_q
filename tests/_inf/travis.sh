#!/usr/bin/env bash

set -e

echo condor_version
pytest -n 6 --cov
