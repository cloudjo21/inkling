#!/bin/bash

PKG_VERSION=$(python setup.py --version)
PKG_NAME=inkling

if [[ $PKG_VERSION == *".dev"* ]]; then
  ./deploy.sh
  ./install.sh $PKG_VERSION
  echo "inkling-${INKLING_VERSION} installation completed!"
else
  echo "inkling-${INKLING_VERSION} installation failed."
fi

