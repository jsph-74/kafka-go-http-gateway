FROM golang:1.24-alpine

WORKDIR /app

# Copy producer source code
COPY src/producer/ ./

# Download dependencies
RUN go mod download

# Build the producer
RUN go build -o go-producer .

# Expose port
EXPOSE 6969

# Run the producer
CMD ["./go-producer"]