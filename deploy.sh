#!/bin/sh -xue

DEPLOY_DIR=deploy/cosyandb-$VERSION

rm -rf $DEPLOY_DIR
mkdir -p $DEPLOY_DIR
gradle fatJar
cp build/libs/cosyan-all.jar $DEPLOY_DIR/
cp -r web $DEPLOY_DIR/
cp start.sh $DEPLOY_DIR/
cp stop.sh $DEPLOY_DIR/
cp restart.sh $DEPLOY_DIR/
cp -r conf $DEPLOY_DIR/
cd deploy/
tar -czf ~/cosyandb-$VERSION.tar.gz cosyandb-$VERSION
