#!/bin/bash

export INKLING_VERSION=$1
export PKG_SERVER_HOST=0.0.0.0
export PKG_SERVER_PORT=8099

pip -v -q install --extra-index-url http://${PKG_SERVER_HOST}:${PKG_SERVER_PORT}/simple --trusted-host=${PKG_SERVER_HOST} inkling==${INKLING_VERSION}

echo "inkling-${INKLING_VERSION} installation completed!"
