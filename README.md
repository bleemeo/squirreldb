# SquirrelDB

## Test and Develop

If you want to install from source and or develop on SquirrelDB, here are the step to run from a git checkout:

Those step of made for Ubuntu 19.04 or more, but appart the installation of Golang 1.12 the same step should apply on any systems.

- Install Golang 1.12, activate it and install golangci-lint (this is needed only once):

```shell script
sudo apt install golang-1.12
export PATH=/usr/lib/go-1.12/bin:$PATH

(cd /tmp; GO111MODULE=on go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.22.2)
```

- If not yet done, activate Golang 1.12:

```
export PATH=/usr/lib/go-1.12/bin:$PATH
```

- Run development version:

```shell script
go run squirreldb
```

- Check for errors and maintainability of the source code:

```shell script
~/go/bin/golangci-lint run
go test squirreldb/...
```

### Note on VS code

SquirrelDB use Go module. VS code support for Go module require usage of gopls.
Enable "Use Language Server" in VS code option for Go.

To install or update gopls, use:

```
(cd /tmp; GO112MODULE=on go get golang.org/x/tools/gopls@latest)
```