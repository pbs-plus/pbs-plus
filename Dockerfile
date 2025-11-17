FROM golang:1.25 as builder

# Set the working directory
WORKDIR /app

# Copy the Go modules and source code
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Build the application
RUN GOOS=linux go build -o pbs-plus-agent ./cmd/unix_agent

FROM debian:bookworm-slim

# Create the sources.list file and enable contrib
RUN apt-get update && \
  apt-get install -y util-linux btrfs-progs lvm2 ca-certificates && \
  rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /app/pbs-plus-agent /usr/local/bin/pbs-plus-agent

VOLUME [ "/etc/pbs-plus-agent" ]

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/pbs-plus-agent"]
