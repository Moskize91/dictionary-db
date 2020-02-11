#!/bin/bash
set -exo pipefail
BASEDIR=$(cd $(dirname "$0"); cd ../; pwd -P)

cd $BASEDIR/dictionary-db
yarn build

cd $BASEDIR/tablestore-db
yarn build