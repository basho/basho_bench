FROM erlang:20-alpine
ARG deployment
ADD . /srv/basho_bench
WORKDIR /srv/basho_bench
RUN rebar3 do upgrade, compile, escriptize

