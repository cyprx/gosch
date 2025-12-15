FROM golang:1.25-alpine

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN apk add --no-cache gcc musl-dev
RUN go mod download

COPY . .

# Run tests
CMD ["go", "test", "-v", "./..."]
