FROM golang:alpine as builder
RUN apk add \
    git \
    gcc \
    g++ \
    linux-headers \
    build-base
RUN mkdir /build
WORKDIR /build
COPY go.mod .
COPY go.sum .
RUN go mod download
ADD . /build/
RUN make yarnbeat

FROM alpine
RUN adduser -S -D -h /app appuser
USER appuser
COPY --from=builder /build/bin/yarnbeat /app/
WORKDIR /app
ENTRYPOINT ["./yarnbeat", "-e", "-c", "/etc/yarnbeat/yarnbeat.yml"]