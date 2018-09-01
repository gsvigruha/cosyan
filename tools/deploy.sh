#!/bin/sh -xue

cd $(git rev-parse --show-toplevel)
DEPLOY_DIR=deploy/cosyandb-$VERSION

rm -rf $DEPLOY_DIR
mkdir -p $DEPLOY_DIR
gradle unitTest
gradle fatJar
cp build/libs/cosyan-all.jar $DEPLOY_DIR/
cp -r web $DEPLOY_DIR/
cp tools/start.sh $DEPLOY_DIR/
cp tools/stop.sh $DEPLOY_DIR/
cp tools/restart.sh $DEPLOY_DIR/
cp -r conf $DEPLOY_DIR/
cd deploy/
tar -czf ~/cosyandb-$VERSION.tar.gz cosyandb-$VERSION
