#!/usr/bin/env bash
pip install virtualenv

virtualenv ../venv

source ../venv/Scripts/activate

pip install -r ../requirements.txt -t ../venv