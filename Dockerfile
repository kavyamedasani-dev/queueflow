# ---------- Build stage ----------
FROM golang:1.26.5-alpine AS builder

WORKDIR /app

# Copy Go dependency files first
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the QueueFlow source code
COPY . .

# Build the QueueFlow application
RUN CGO_ENABLED=0 GOOS=linux go build -o queueflow .


# ---------- Runtime stage ----------
FROM alpine:latest

WORKDIR /app

# Install CA certificates
RUN apk --no-cache add ca-certificates

# Copy the compiled QueueFlow binary from the build stage
COPY --from=builder /app/queueflow .

# QueueFlow API runs on port 8080
EXPOSE 8080

# Start QueueFlow
CMD ["./queueflow"]