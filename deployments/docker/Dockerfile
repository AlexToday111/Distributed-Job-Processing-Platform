FROM golang:1.26.7-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/api ./cmd/api && \
    CGO_ENABLED=0 go build -trimpath -o /out/scheduler ./cmd/scheduler && \
    CGO_ENABLED=0 go build -trimpath -o /out/worker ./cmd/worker

FROM build AS test
RUN apk add --no-cache build-base
CMD ["go", "test", "-race", "./..."]

FROM alpine:3.23 AS runtime
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
COPY --from=build /out/ /usr/local/bin/
USER app
CMD ["api"]
