build:
	go build

clean:
	rm -rf dq/
	rm -rf bin/dq*

build-macos:
	GOOS=darwin GOARCH=amd64 go build -o bin/dq_darwin-amd64
	GOOS=darwin GOARCH=arm64 go build -o bin/dq_darwin-arm64

build-linux:
	GOOS=linux GOARCH=amd64 go build -o bin/dq_linux-amd64

build-windows:
    GOOS=windows GOARCH=amd64 go build -o bin/dq_windows-amd64

build-all: clean build-macos build-linux build-windows

compress-linux:
	upx ./bin/dq_linux*