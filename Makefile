PROG_NAME=key-value

build:
	GOARCH=amd64 GOOS=darwin go build -o ${PROG_NAME}-darwin main.go
	GOARCH=amd64 GOOS=linux go build -o ${PROG_NAME}-linux main.go
	GOARCH=amd64 GOOS=window go build -o ${PROG_NAME}-windows main.go

clean:
	go clean
	rm ${PROG_NAME}-darwin
	rm ${PROG_NAME}-linux
	rm ${PROG_NAME}-windows

run:
	go run main.go

test:
	go test -v