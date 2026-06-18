FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /out/rtk-cloud-logger ./cmd/rtk-cloud-logger

FROM alpine:3.20

RUN adduser -D -H -u 10001 appuser
COPY --from=builder /out/rtk-cloud-logger /usr/local/bin/rtk-cloud-logger
USER 10001:10001
EXPOSE 18090
ENTRYPOINT ["/usr/local/bin/rtk-cloud-logger"]
CMD ["-addr", ":18090"]
