# Install

## By binary files (recommended)

Download the binary files from [release](https://github.com/housepower/clickhouse_sinker/releases), choose the executable binary file according to your env.

## By container image

`docker run --volume conf:/etc/clickhouse_sinker quay.io/housepower/clickhouse_sinker`

## By source

- Install Golang

- Go Get

```
go get -u github.com/housepower/clickhouse_sinker/...
```

- Build && Run

```
make build
```
