ifeq ($(origin II_SYSTEM),undefined)
    $(error II_SYSTEM variable should be set)
else
    $(info II_SYSTEM: using $(II_SYSTEM))
endif

makeFileDir := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: all
all: lib

iiapi.pc: iiapi.pc.template
	sed "s~II_SYSTEM~${II_SYSTEM}~g" iiapi.pc.template > $@

lib: conn.go openapi.go iiapi.pc
	PKG_CONFIG_PATH=${makeFileDir} go build

test: conn.go openapi.go openapi_test.go iiapi.pc
	PKG_CONFIG_PATH=${makeFileDir} go test
