FROM golang:1.26-alpine AS build

WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvnode ./cmd/kvnode \
    && mkdir -p /out/data \
    && chown 65532:65532 /out/data

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/kvnode /kvnode
COPY --from=build --chown=65532:65532 /out/data /data
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["/kvnode"]
