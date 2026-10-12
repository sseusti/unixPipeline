# Concurrent Email Processing Pipeline

A small Go project that builds a Unix-style processing pipeline with goroutines and channels. It looks up users by email, fetches their messages in batches, checks each message for spam, and returns a sorted list of results.

The project is a concurrency exercise rather than a standalone command-line application. Its simulated services deliberately take time to respond, so the tests exercise streaming, parallel work, batching, synchronization, and safe channel shutdown.

## Requirements

- Go version specified by [`go.mod`](go.mod)
- Network access the first time tests are run, so Go can download the test dependencies

The application code uses only the Go standard library. Tests use [`testify`](https://github.com/stretchr/testify).

## Run the tests

From the project root, run:

```sh
go test -v -race ./spammer
```

The race detector checks for unsynchronized access to shared state. The verbose test output includes logs from the simulated services. The full test suite takes several seconds because those services intentionally sleep.

## Project structure

```text
.
├── go.mod
├── go.sum
├── README.md
└── spammer/
    ├── common.go      # Data types and simulated user, message, and spam services
    ├── spammer.go     # Pipeline runner and processing stages
    └── main_test.go   # Pipeline, concurrency, and end-to-end tests
```

## How it works

`RunPipeline` connects processing functions with channels and runs each stage in its own goroutine. When a stage returns, the runner closes that stage's output channel. The next stage reads until the channel closes, and the runner waits for every stage to finish.

The stages in `spammer/spammer.go` process values in this order:

1. **`SelectUsers`** calls `GetUser` concurrently for incoming email addresses. It removes duplicate users by ID, including aliases such as `batman@mail.ru` and `bruce.wayne@mail.ru`.
2. **`SelectMessages`** groups users into batches of up to two and calls `GetMessages` concurrently for each batch. It also handles a final batch containing just one user.
3. **`CheckSpam`** calls `HasSpam` for each message. A buffered channel acts as a semaphore to keep the number of simultaneous requests at or below five.
4. **`CombineResults`** collects all spam results, sorts spam messages before non-spam messages and sorts each group by message ID, then emits lines such as `true 123`.

Each stage streams results to the next as soon as it can. `CombineResults` is the exception: it must collect all results before it can sort them. The simulated service functions and shared test counters are defined in `spammer/common.go`.

## What the tests cover

- Channels carry values through the pipeline without waiting for an entire stage to finish.
- User aliases produce only one output user, with no data races.
- Message lookups use batches of at most two users and separate pipelines can run at the same time.
- Spam checks respect the service's five-request concurrency limit.
- The complete pipeline returns the expected sorted output, makes the expected number of service calls, and finishes within the time limit.
