FROM golang:1.24-alpine

WORKDIR /app

# Copy consumer source code
COPY src/consumer/ ./

# Download dependencies
RUN go mod download

# Build the consumer
RUN go build -o go-consumer .

# Run the consumer
CMD ["./go-consumer"]