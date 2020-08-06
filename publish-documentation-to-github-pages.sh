#!/bin/sh

MESSAGE=$(git log -1 --pretty=%B)
./mvnw asciidoctor:process-asciidoc

# Concourse does shallow clones, so need the next 2 commands to have the gh-pages branch
git remote set-branches origin 'gh-pages'
git fetch -v
git checkout gh-pages
mkdir -p snapshot/htmlsingle
cp target/generated-docs/index.html snapshot/htmlsingle
git add snapshot/
git commit -m "$MESSAGE"
git push origin gh-pages
git checkout master
