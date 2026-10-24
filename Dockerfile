FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /scheduler .

FROM alpine:3.22
RUN adduser -D -u 10001 scheduler
USER scheduler
COPY --from=build /scheduler /usr/local/bin/scheduler
ENTRYPOINT ["scheduler"]
