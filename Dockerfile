FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o libvirt-exporter

FROM alpine

COPY --from=builder /app/libvirt-exporter /libvirt-exporter

EXPOSE 9177

ENTRYPOINT ["/libvirt-exporter", "-listen", "0.0.0.0:9177"]
