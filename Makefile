.PHONY: deps

all: deps compile escriptize

deps:
	@./rebar get-deps

compile: deps
	@./rebar compile

clean:
	@./rebar clean

distclean: clean
	@rm -rf deps read_repair

escriptize: deps compile
	@./rebar skip_deps=true escriptize
