name: Test Meta Ads Credentials

on:
  workflow_dispatch:

permissions:
  contents: read

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '20'
      - name: Run test
        env:
          META_APP_ID: ${{ secrets.META_APP_ID }}
          META_APP_SECRET: ${{ secrets.META_APP_SECRET }}
          META_SYSTEM_USER_TOKEN: ${{ secrets.META_SYSTEM_USER_TOKEN }}
          META_AD_ACCOUNT_IDS: ${{ secrets.META_AD_ACCOUNT_IDS }}
        run: node test-meta.js
