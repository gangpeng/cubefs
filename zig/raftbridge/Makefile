# Makefile for the Go raft bridge library.
# Builds libcubefs_raft.so from the Go source using cgo c-shared buildmode.

.PHONY: all clean install

GOFLAGS := -buildmode=c-shared
OUTPUT := libcubefs_raft.so
HEADER := libcubefs_raft.h
INSTALL_DIR := ../lib

all: $(OUTPUT)

$(OUTPUT): main.go callbacks.go types.go go.mod
	@echo "Building $(OUTPUT)..."
	cd $(CURDIR) && go build $(GOFLAGS) -o $(OUTPUT) .
	@echo "Built $(OUTPUT) successfully."

clean:
	rm -f $(OUTPUT) $(HEADER)

install: $(OUTPUT)
	@mkdir -p $(INSTALL_DIR)
	cp $(OUTPUT) $(INSTALL_DIR)/
	@if [ -f $(HEADER) ]; then cp $(HEADER) $(INSTALL_DIR)/; fi
	@echo "Installed to $(INSTALL_DIR)/"
