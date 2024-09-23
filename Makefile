# Variables
APP_NAME := 1brc

all: build run

build:
	@echo "Building $(APP_NAME)..."
	go build -o $(APP_NAME) 
	@echo "Build complete."

run:
	./$(APP_NAME)

clean:
	@echo "Cleaning up..."
	rm -f $(APP_NAME)
	@echo "Clean complete."

.PHONY: all build run clean
