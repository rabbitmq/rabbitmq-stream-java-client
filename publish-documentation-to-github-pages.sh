#!/bin/sh

MESSAGE=$(git log -1 --pretty=%B)
./mvnw clean buildnumber:create pre-site

./mvnw javadoc:javadoc

# Concourse does shallow clones, so need the next 2 commands to have the gh-pages branch
git remote set-branches origin 'gh-pages'
git fetch -v
git checkout gh-pages
mkdir -p snapshot/htmlsingle
cp target/generated-docs/index.html snapshot/htmlsingle
mkdir -p snapshot/pdf
cp target/generated-docs/index.pdf snapshot/pdf
mkdir -p snapshot/api
cp -r target/site/apidocs/* snapshot/api/
git add snapshot/
git commit -m "$MESSAGE"
git push origin gh-pages
git checkout master
