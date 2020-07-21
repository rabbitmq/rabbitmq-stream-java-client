#!/bin/sh

MESSAGE=$(git log -1 --pretty=%B)
./mvnw asciidoctor:process-asciidoc

# Concourse does shallow clones, so need the next 2 commands to have the gh-pages branch
git remote set-branches origin 'gh-pages'
git fetch -v
git checkout gh-pages
mkdir -p api-spike/htmlsingle
cp target/generated-docs/index.html api-spike/htmlsingle
git add api-spike/
git commit -m "$MESSAGE"
git push origin gh-pages
git checkout master
