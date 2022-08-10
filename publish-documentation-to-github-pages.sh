#!/bin/bash

. $(pwd)/release-versions.txt

MESSAGE=$(git log -1 --pretty=%B)
./mvnw clean buildnumber:create pre-site

./mvnw javadoc:javadoc -Dmaven.javadoc.skip=false

RELEASE_VERSION="sac"

git checkout -- .mvn/maven.config

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

git commit -m "$MESSAGE"
git push origin gh-pages
git checkout main