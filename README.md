https://hub.docker.com/r/bitnami/kafka/

# to run kafka:
docker-compose up

# run writer:
go run writer/writer.go

# run reader:
go run reader/reader.go