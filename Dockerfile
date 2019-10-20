# Build stage 0
FROM erlang:alpine

# Set working directory
RUN mkdir -p /buildroot/spg
WORKDIR /buildroot/spg

# Copy our Erlang test application
COPY ./src ./src
COPY ./test ./test
COPY ./rebar.config ./rebar.config
COPY ./rebar.lock ./rebar.lock

# Replace spg_sup with the test version of it
COPY ./test/stateless_service_SUITE_data/test_sup.erl.template ./src/spg_sup.erl

# And build the release
# WORKDIR ./
RUN rebar3 as docker release

# Build stage 1
#FROM alpine

# Install some libs
RUN apk add --no-cache openssl && \
    apk add --no-cache ncurses-libs

# Install the released application
#COPY --from=0 /buildroot/spg/_build/prod/rel/spg /spg
#COPY /buildroot/spg/_build/prod/rel/spg /spg

# Testing port 4368 is exposed via command line
# EXPOSE 4368

# Expose 'dist' port for control node
# EXPOSE 4370

CMD ["/buildroot/spg/_build/docker/rel/spg/bin/spg"]

#CMD ["/spg/bin/spg", "console"]
