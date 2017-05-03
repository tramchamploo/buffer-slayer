#!/bin/bash

set -eo pipefail

if [[ $GIT_USER_EMAIL ]]; then
  git config --global user.email "$GIT_USER_EMAIL"
fi

if [[ $GIT_USER_NAME ]]; then
  git config --global user.name "$GIT_USER_NAME"
fi

if [[ $RELEASE ]]; then
  mvn -B release:prepare -DreleaseVersion=$RELEASE -DdevelopmentVersion=$NEXT -DskipTests
  mvn -B release:perform --settings=".buildscript/settings.xml" -DskipTests
  sed -i "s/<version>\(.*\)</<version>$RELEASE</" README.md
fi
