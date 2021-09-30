build:
	GOARCH=amd64 go build -mod=vendor -a -installsuffix cgo -ldflags '-w' -o bin/map cmd/main.go
.PHONY: build

clean:
	make clean
.PHONY: clean

release: build
	docker build -t docker.io/zhangbo1882/map
.PHONY: release

push: release
	docker push docker.io/zhangbo1882/map
.PHONY: push
