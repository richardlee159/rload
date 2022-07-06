FROM rust:1.62.0 as builder
RUN apt-get update && apt-get install -y libluajit-5.1-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /usr/src/rload
COPY . .
RUN cargo install --path .
 
FROM debian:stable-slim
RUN apt-get update && apt-get install -y libluajit-5.1-2 luarocks \
    && rm -rf /var/lib/apt/lists/*  && luarocks install luasocket 3.0rc1
COPY --from=builder /usr/local/cargo/bin/rload /usr/local/bin/rload
CMD ["rload"]