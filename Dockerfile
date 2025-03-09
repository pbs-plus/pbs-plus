FROM golang:1.24 as builder

# Set the working directory
WORKDIR /app

# Copy the Go modules and source code
COPY go.mod go.sum ./
RUN go mod download
COPY . .

# Build the application
RUN GOOS=linux go build -o pbs-plus-agent ./cmd/linux_agent

FROM debian:bookworm-slim

# Install required tools and dependencies
RUN sed -r -i'.BAK' 's/^deb(.*)$/deb\1 contrib/g' /etc/apt/sources.list && apt-get update && apt-get install -y zfsutils-linux btrfs-progs lvm2 ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /app/pbs-plus-agent /usr/local/pbs-plus-agent

VOLUME [ "/etc/pbs-plus-agent" ]

# Set the entrypoint
ENTRYPOINT ["/usr/local/pbs-plus-agent"]
