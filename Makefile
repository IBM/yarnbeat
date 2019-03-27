BEATS_PATH = $(GOPATH)/pkg/mod/github.com/elastic/beats@v6.6.2+incompatible
BEATS_VERSION = 6.6.2
BEAT_NAME = yarnbeat
BUILD_PATH = ./bin


deps:
	go mod download

test: deps
	go test ./...

testcov: deps
	go test -race -coverprofile=profile.out -covermode=atomic ./...
	mv profile.out coverage.txt

generate-fields: deps
	go run ${BEATS_PATH}/dev-tools/cmd/asset/asset.go  -in _meta/fields.yml -out include/fields.go  -pkg include -name fields.yml ${BEAT_NAME}

$(BEAT_NAME): generate-fields dashboards
	go build -o ${BUILD_PATH}/${BEAT_NAME}

docker: generate-fields
	docker build -t ${BEAT_NAME} .

install: generate-fields
	go install

dashboards: deps
	mkdir -p ${BUILD_PATH}
	rm -rf ${BUILD_PATH}/kibana
	cp -r kibana ${BUILD_PATH}
	go run ${BEATS_PATH}/dev-tools/cmd/kibana_index_pattern/kibana_index_pattern.go  -beat ${BEAT_NAME} -version ${BEATS_VERSION} -index ${BEAT_NAME}-* -fields _meta/fields.yml -out ${BUILD_PATH}/kibana

clean:
	rm -rf ${BUILD_PATH}