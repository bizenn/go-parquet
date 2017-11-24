define thrift
	docker run --rm -v "$$PWD:/data" -w /data thrift thrift --gen go -o $2 $1
	go fmt ./gen-go/parquet
endef

.PHONY: test submodules

test:
	@go test -v .

gen-go/parquet/parquet.go: parquet-format/src/main/thrift/parquet.thrift
	$(call thrift,$^,.)

submodules:
	git submodule update -i
