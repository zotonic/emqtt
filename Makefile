all: deps compile

.PHONY: compile test

compile: deps
	./rebar compile

deps:
	./rebar get-deps

clean:
	./rebar clean

generate:
	./rebar generate -f

relclean:
	rm -rf rel/emqtt

test:
	./rebar get-dep compile
	./rebar eunit -v skip_deps=true
