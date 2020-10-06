## Docker setup:

`docker run -p 3306:3306 --name mysql -e MYSQL_ROOT_PASSWORD=root -d mysql`

and

`docker exec -it mysql mysql -uroot -p`

## How to run
load dataset: <br>
`go run main.go tasks.go --op load` <br>

run exercises: <br>
`go run main.go tasks.go --op exercises` <br>

drop tables: <br>
`go run main.go tasks.go --op drop` <br>