#!/bin/bash

set -eo pipefail

if [[ $GIT_USER_EMAIL ]]; then
  git config --global user.email "$GIT_USER_EMAIL"
fi

if [[ $GIT_USER_NAME ]]; then
  git config --global user.name "$GIT_USER_NAME"
fi

if [[ $RELEASE ]]; then
  gpg --keyserver hkp://pool.sks-keyservers.net --recv-keys 2E52AC87
  # Checkout before commit
  git pull origin
  mvn -B release:prepare --settings=".buildscript/settings.xml" -Prelease -DreleaseVersion=$RELEASE -DdevelopmentVersion=$NEXT -Darguments=-DskipTests
  mvn -B release:perform --settings=".buildscript/settings.xml" -Prelease -Darguments=-DskipTests
  # Modify version in README.md
  sed -i "s/<version>\(.*\)</<version>$RELEASE</" README.md
  git add README.md
  git commit -m "Update README at $(date)"
  git push origin master
fi