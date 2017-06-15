#!/bin/bash

set -eo pipefail

if [[ $GIT_USER_EMAIL ]]; then
  git config --global user.email "$GIT_USER_EMAIL"
fi

if [[ $GIT_USER_NAME ]]; then
  git config --global user.name "$GIT_USER_NAME"
fi

if [[ $RELEASE ]]; then
  # Generate a gpg key
  cat > gen-key-script <<EOF
       Key-Type: RSA
       Subkey-Type: RSA
       Name-Real: buffer-slayer
       Name-Comment: buffer-slayer deployment key
       Name-Email: tramchamploo@gmail.com
       Expire-Date: 0
       Passphrase: $GPG_PASSPHRASE
EOF
  gpg --batch --gen-key gen-key-script
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