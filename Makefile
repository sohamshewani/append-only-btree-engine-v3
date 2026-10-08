CC = gcc
CFLAGS = -O3 -Wall -Wextra -std=c11 -pedantic
LDFLAGS = -lpthread

SRC_DIR = src
INC_DIR = include
BUILD_DIR = build
TEST_DIR = tests
BENCH_DIR = benchmarks

SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(patsubst $(SRC_DIR)/%.c, $(BUILD_DIR)/%.o, $(SRCS))

TARGET = $(BUILD_DIR)/append-only-btree-engine

.PHONY: all build test clean benchmark

all: build

build:
	@mkdir -p $(BUILD_DIR)
	@if [ -n "$(SRCS)" ]; then \
		$(MAKE) $(TARGET); \
	else \
		echo "No source files found in $(SRC_DIR). Creating dummy executable."; \
		touch $(TARGET); \
	fi

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -I$(INC_DIR) $^ -o $@ $(LDFLAGS)

$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(BUILD_DIR)
	$(CC) $(CFLAGS) -I$(INC_DIR) -c $< -o $@

test: build
	@mkdir -p $(BUILD_DIR)
	@if [ -d "$(TEST_DIR)" ] && [ -n "$$(ls -A $(TEST_DIR) 2>/dev/null)" ]; then \
		echo "Running tests..."; \
		$(CC) $(CFLAGS) -I$(INC_DIR) $(TEST_DIR)/*.c $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(BUILD_DIR)/test_runner $(LDFLAGS); \
		./$(BUILD_DIR)/test_runner; \
	else \
		echo "No tests found in $(TEST_DIR). Skipping."; \
	fi

benchmark: build
	@mkdir -p $(BUILD_DIR)
	@if [ -d "$(BENCH_DIR)" ] && [ -n "$$(ls -A $(BENCH_DIR) 2>/dev/null)" ]; then \
		echo "Running benchmarks..."; \
		$(CC) $(CFLAGS) -I$(INC_DIR) $(BENCH_DIR)/*.c $(filter-out $(BUILD_DIR)/main.o, $(OBJS)) -o $(BUILD_DIR)/bench_runner $(LDFLAGS); \
		./$(BUILD_DIR)/bench_runner; \
	else \
		echo "No benchmarks found in $(BENCH_DIR). Skipping."; \
	fi

clean:
	@rm -rf $(BUILD_DIR)
	@echo "Cleaned build artifacts."