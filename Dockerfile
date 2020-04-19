# Base image
FROM rust:alpine as builder
WORKDIR /usr/src/cord
COPY . .
ENV RUSTFLAGS="-C target-feature=-crt-static"
RUN apk update && \
    apk upgrade && \
    apk add --update alpine-sdk && \
    cargo install --path .

# Client CLI
FROM alpine
COPY --from=builder /usr/local/cargo/bin/cord-client /usr/local/bin/cord-client
RUN apk update && \
    apk upgrade && \
    apk add --update libgcc
ENTRYPOINT ["cord-client"]
CMD ["--help"]
