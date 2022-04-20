GO_BUILD=go build
GO_TEST=go clean -testcache && go test -v -race
GO_CLEAN=go mod tidy && go mod clean
GO_MOD_TIDY=go mod tidy
IS_RELEASE=false
DEBUG_GO_BUILD_OPTIONS=-race
RELEASE_GO_BUILD_OPTIONS=
GO_BUILD_OPTIONS=$(DEBUG_GO_BUILD_OPTIONS)
DEBUG_GLOBAL_LDFLAGS=
RELEASE_GLOBAL_LDFLAGS=-ldflags "-s -w"
LDFLAGS_GLOBAL=${DEBUG_GLOBAL_LDFLAGS}

all: go-build

.PHONY: release
release:
	$(info RELEASE FLAGS)
	$(eval IS_RELEASE = true)
	$(eval LDFLAGS_GLOBAL = $(RELEASE_GLOBAL_LDFLAGS))
	$(eval GO_BUILD_OPTIONS = $(RELEASE_GO_BUILD_OPTIONS))
	@: # this suppresses message nothing to do for target

.PHONY: go-build
go-build:
	$(GO_BUILD) $(GO_BUILD_OPTIONS) $(LDFLAGS_GLOBAL) ./... ; \

.PHONY: test
test:
	$(GO_TEST) ${LDFLAGS_GLOBAL} -v ./...

# G304 - machineid/helper.go:31 false positive on file name in variable
# G404 - weak random generator for exponential backoff times is fine (backoff/backoff.go:71)
# G402 - TLS InsecureSkipVerify set true on purpose used by httpcommon for http probing through a proxy
.PHONY: gosec
gosec:
	gosec -exclude=G304,G404,G402 ./...

.PHONY: clean
clean:
	$(GO_CLEAN)

# FIXME remove exclude for "-D staticcheck" when deprecated logging API usage is fixed
.PHONY: golint
golint:
	golangci-lint run -D govet -D staticcheck

GO_MODULES += "go.uber.org/zap"
GO_MODULES += "golang.org/x/sync/semaphore"
GO_MODULES += "google.golang.org/grpc/codes"
GO_MODULES += "google.golang.org/grpc/status"
GO_MODULES += "github.com/elazarl/goproxy"
GO_MODULES += "howett.net/plist"
GO_MODULES += "go.mongodb.org/mongo-driver"
GO_MODULES += "google.golang.org/grpc"
GO_MODULES += "google.golang.org/grpc"
GO_MODULES += "google.golang.org/grpc/credentials"
GO_MODULES += "google.golang.org/grpc/keepalive"
GO_MODULES += "google.golang.org/grpc/security/advancedtls"
GO_MODULES += "github.com/pkg/errors"
GO_MODULES += "github.com/natefinch/lumberjack"
GO_MODULES += "github.com/mattn/go-colorable"
GO_MODULES += "github.com/sirupsen/logrus"
GO_MODULES += "github.com/stealthmodesoft/service@0c1cf24"
GO_MODULES += "github.com/jpillora/backoff"

GO_MODULES_TOOLS += "github.com/securego/gosec/v2/cmd/gosec"

.PHONY: setup
setup:
	for m in $(GO_MODULES); do \
			echo "go get -u $$m"; \
			go get -u $$m; \
	done

	for m in $(GO_MODULES_TOOLS); do \
			echo "go get -d $$m"; \
			go get -d $$m; \
	done
	$(GO_MOD_TIDY)


