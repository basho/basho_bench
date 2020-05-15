ARG deployment
FROM registry.gitlab.com/riak/riak/${deployment}:latest
ARG deployment
ADD . /srv/basho_bench
WORKDIR /srv/basho_bench
RUN rebar3 do upgrade, compile, escriptize

