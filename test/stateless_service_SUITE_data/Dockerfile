# Build stage 0
FROM erlang:alpine

# Set working directory
RUN mkdir -p /buildroot/spg
WORKDIR /buildroot/spg

# Copy our Erlang test application
COPY ./ ./

# And build the release
RUN rebar3 release

# Build stage 1
#FROM alpine

# Install openssl & ncurses (BEAM won't start otherwise)
RUN apk add --no-cache openssl && \
    apk add --no-cache ncurses-libs

# Install the released application
#COPY --from=0 /buildroot/spg/_build/prod/rel/spg /spg
#COPY /buildroot/spg/_build/prod/rel/spg /spg

CMD ["/buildroot/spg/_build/default/rel/spg/bin/spg"]

#CMD ["/spg/bin/spg", "console"]
