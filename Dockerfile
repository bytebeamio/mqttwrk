FROM rust:1.65 AS build

# Cache build
# https://dev.to/rogertorres/first-steps-with-docker-rust-30oi
WORKDIR /usr/share/bytebeam/mqttwrk
RUN cargo init --bin

COPY ./Cargo.toml .
RUN cargo build --release

# Actual build
COPY . /usr/share/bytebeam/mqttwrk
WORKDIR /usr/share/bytebeam/mqttwrk

RUN rm ./target/release/deps/mqttwrk*
RUN cargo build --release

# ---------------------------------------

FROM ubuntu:22.04

ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

RUN apt-get -y upgrade
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y sudo curl runit vim wget gnupg2 lsb-release python3-pip

COPY runit/ /etc/runit
RUN rm -rf /etc/runit/runsvdir

COPY --from=build /usr/share/bytebeam/mqttwrk /usr/share/bytebeam/mqttwrk
WORKDIR /usr/share/bytebeam/mqttwrk
RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["/usr/bin/runsvdir", "/etc/runit"]
