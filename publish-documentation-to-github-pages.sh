#!/bin/bash

. $(pwd)/release-versions.txt

MESSAGE=$(git log -1 --pretty=%B)
./mvnw clean buildnumber:create pre-site

./mvnw javadoc:javadoc

RELEASE_VERSION=$(cat pom.xml | grep -oPm1 "(?<=<version>)[^<]+")

# Concourse does shallow clones, so need the next 2 commands to have the gh-pages branch
git remote set-branches origin 'gh-pages'
git fetch -v

git checkout gh-pages
mkdir -p $RELEASE_VERSION/htmlsingle
cp target/generated-docs/index.html $RELEASE_VERSION/htmlsingle
mkdir -p $RELEASE_VERSION/pdf
cp target/generated-docs/index.pdf $RELEASE_VERSION/pdf
mkdir -p $RELEASE_VERSION/api
cp -r target/site/apidocs/* $RELEASE_VERSION/api/
git add $RELEASE_VERSION/

if [[ $LATEST == "true" ]]
  then
    if [[ $RELEASE_VERSION == *[RCM]* ]]
  then
    DOC_DIR="milestone"
  elif [[ $RELEASE_VERSION == *SNAPSHOT* ]]
  then
    DOC_DIR="snapshot"
  else
    DOC_DIR="stable"
  fi

  mkdir -p $DOC_DIR/htmlsingle
  cp target/generated-docs/index.html $DOC_DIR/htmlsingle
  mkdir -p $DOC_DIR/pdf
  cp target/generated-docs/index.pdf $DOC_DIR/pdf
  mkdir -p $DOC_DIR/api
  cp -r target/site/apidocs/* $DOC_DIR/api/
  git add $DOC_DIR/

fi

git commit -m "$MESSAGE"
git push origin gh-pages
git checkout main
