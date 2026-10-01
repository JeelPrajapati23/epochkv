# One image for both roles in the network-emulation setup: kv_server nodes,
# and the client container that runs the benchmark harness (it has to sit on
# the same Docker network, because cluster nodes announce their container
# IPs in MOVED redirects). Built from source, so it doesn't depend on the
# host's build or libraries.

FROM ubuntu:24.04 AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ make cmake git ca-certificates \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY CMakeLists.txt ./
COPY include include
COPY src src
# The top-level CMakeLists adds the tests (and fetches Catch2), so they're
# needed to configure, even though only kv_server is built.
COPY tests tests
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
 && cmake --build build --target kv_server -j"$(nproc)"

FROM ubuntu:24.04
# iproute2: tc, for netem. procps: ps/pkill when debugging inside a container.
RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 python3 procps \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/build/src/kv_server /usr/local/bin/kv_server
WORKDIR /opt/epochkv
COPY bench bench
COPY python-client python-client
COPY tools tools
