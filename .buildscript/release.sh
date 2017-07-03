#!/bin/bash

set -o pipefail

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
  rm gen-key-script

  # Publish public key to key server
  key_id=$(gpg --list-keys | grep -B1 buffer-slayer | head -1 | awk '{print $2}' | awk -F '/' '{print $2}')
  gpg --keyserver hkp://keyserver.ubuntu.com --send-keys $key_id
  gpg --keyserver hkp://pool.sks-keyservers.net --send-keys $key_id

  # Checkout before commit
  git pull origin

  mvn -B release:prepare --settings=".buildscript/settings.xml" -Prelease -DreleaseVersion=$RELEASE -DdevelopmentVersion=$NEXT -Darguments="-DskipTests"

  if [ $? -eq 0 ]; then
    mvn -B release:perform --settings=".buildscript/settings.xml" -Prelease -Darguments="-DskipTests"
  else
    echo "Error preparing a release, rolling back..."
    mvn -B release:rollback
    exit 1
  fi
  # Modify version in README.md
  sed -i "s/<version>\(.*\)</<version>$RELEASE</" README.md
  git add README.md
  git commit -m "Update README at $(date)"
  git push origin master
fi