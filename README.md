# SquirrelDB

## Test and Develop

If you want to install from source and or develop on SquirrelDB, here are the step to run from a git checkout:

- Install Golang 1.12 and activate it (this is needed only once):

```shell script
sudo apt install golang-1.12
export PATH=/usr/lib/go-1.12/bin:$PATH
```

- Install golangci-lint (this is needed only once):

```shell script
go get -u github.com/golangci/golangci-lint/cmd/golangci-lint
```

- Run development version:

```shell script
go run squirreldb
```

- Check for errors and maintainability of the source code:

```shell script
golangci-lint run
```

### Note on VS code

SquirrelDB use Go module. VS code support for Go module require usage of gopls.
Enable "Use Language Server" in VS code option for Go.

To install or update gopls, use:

```
(cd /tmp; GO112MODULE=on go get golang.org/x/tools/gopls@latest)
```