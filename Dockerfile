FROM golang:1.23-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/sentinel-stand ./cmd/sentinel-stand

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/sentinel-stand /sentinel-stand

# GRPC_HOST defaults to localhost, which inside a container means binding the
# container's own loopback: the service logs "Server started" and then refuses
# every connection from outside it.
ENV GRPC_HOST=0.0.0.0

EXPOSE 5001
USER nonroot:nonroot
ENTRYPOINT ["/sentinel-stand"]
