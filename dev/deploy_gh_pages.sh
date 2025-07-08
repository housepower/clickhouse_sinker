#!/usr/bin/env bash

cd docs/.vuepress/dist

git config --global user.name  'GitHub Workflow'
git config --global user.email 'dummy@dummy.dummy'

git init
git add -A
git commit -m 'Deploy GitHub Pages'
git push -f https://x-access-token:${GITHUB_TOKEN}@github.com/viru-tech/clickhouse_sinker.git master:gh-pages
