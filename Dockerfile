ARG TARGETARCH
ARG TARGETPLATFORM
ARG TARGETOS
ARG TARGETARCH
ARG TARGETVARIANT


FROM rust:1.51.0 as builder

WORKDIR /app
COPY . .
RUN apt update && \
apt install -y build-essential gcc make cmake cmake-gui cmake-curses-gui libssl-dev

RUN cargo build --release

FROM debian

WORKDIR /app
RUN apt update && \
apt install -y build-essential gcc make cmake cmake-gui cmake-curses-gui libssl-dev
COPY --from=builder /app/target/release/chronicle ./chronicle

ENTRYPOINT ["/app/chronicle"]
