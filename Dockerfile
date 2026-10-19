FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY gen ./gen
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /stockhold ./cmd/stockhold
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /stockholdctl ./cmd/stockholdctl

FROM alpine:3.22
RUN apk add --no-cache ca-certificates
USER 65532:65532
COPY --from=build /stockhold /stockhold
COPY --from=build /stockholdctl /stockholdctl
EXPOSE 9090
ENTRYPOINT ["/stockhold"]
