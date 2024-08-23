#!/bin/bash

export PKG_NAME=inkling


python setup.py sdist upload -r internal

echo "${PKG_NAME} deployment completed!"
