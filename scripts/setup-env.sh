#!/bin/bash
set -exo pipefail
BASEDIR=$(cd $(dirname "$0"); cd ../; pwd -P)

cd $BASEDIR/../akko-project-utils
scripts/setup-env.sh

cd $BASEDIR
yarn
yarn build