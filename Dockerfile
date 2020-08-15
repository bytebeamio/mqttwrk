# ------------------------------------------------------------------------------
# Cargo Build Stage
# ------------------------------------------------------------------------------

FROM rust:latest as cargo-build
WORKDIR /usr/src/mqttwrk
COPY Cargo.toml Cargo.toml
RUN mkdir src/
COPY . .
RUN cargo build --release
RUN cargo install --path .

# ------------------------------------------------------------------------------
# Final Stage
# ------------------------------------------------------------------------------

FROM ubuntu:latest

COPY --from=cargo-build /usr/local/cargo/bin/mqttwrk /usr/local/bin/mqttwrk

ENTRYPOINT ["mqttwrk"]