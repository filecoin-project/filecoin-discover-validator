DAGGO ?= go

DAGTAG_NOSANCHECKS := nosanchecks_DAGger nosanchecks_qringbuf

.PHONY: $(MAKECMDGOALS)

build: dataset
	@rm -f fil-discover-validator
	@[ -r tmp/data/fil_discover_full.dat ]
	$(DAGGO) build \
		-o fil-discover-validator ./cmd/fil-discover-validator
	$(DAGGO) run github.com/GeertJohan/go.rice/rice append --exec fil-discover-validator -i ./cmd/fil-discover-validator

dataset:
	@mkdir -p tmp/data
	[ -r tmp/data/fil_discover_full.dat ] || curl -Lo tmp/data/fil_discover_full.dat https://fil-discover-drive-validation-bin.s3.ap-east-1.amazonaws.com/fil_discover_full.dat
