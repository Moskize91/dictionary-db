#!/bin/bash
set -exo pipefail
BASEDIR=$(cd $(dirname "$0"); cd ../; pwd -P)

cd $BASEDIR
rabbit clone
./scripts/setup-env.sh

cd $BASEDIR/dictionary-db
yarn run check

cd $BASEDIR/tablestore-db
yarn test