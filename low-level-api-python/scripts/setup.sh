#!/usr/bin/env bash
pip install virtualenv

virtualenv ../venv

source ../venv/bin/activate && pip install -r ../requirements.txt