#!/usr/bin/env bash

cd docs/.vuepress/dist

git config --global user.name  'GitHub Workflow'
git config --global user.email 'dummy@dummy.dummy'

git init
git add -A
git commit -m 'Deploy GitHub Pages'
git push -f https://sundy-li:${TOKEN_GITHUB}@github.com/housepower/clickhouse_sinker.git master:gh-pages
