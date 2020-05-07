#!/bin/sh

MESSAGE=$(git log -1 --pretty=%B)
./mvnw asciidoctor:process-asciidoc
git checkout gh-pages
cp target/generated-docs/index.html snapshot/htmlsingle
git add snapshot/
git commit -m "$MESSAGE"
git push origin gh-pages
git checkout master