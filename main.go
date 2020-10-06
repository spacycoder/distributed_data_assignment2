package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spacycoder/db_mysql/pkg/activity"
	"github.com/spacycoder/db_mysql/pkg/trackpoint"
	"github.com/spacycoder/db_mysql/pkg/user"
)

const dateLayout string = "2006/01/02 15:04:05"
const validLineCount int = 2506

type empty struct{}

type Config struct {
	WorkerCount int
	User        string
	Password    string
	DbURL       string
	Operation   string
}

func main() {
	cpus := runtime.NumCPU()
	fmt.Printf("Number of CPUs: %d\n", cpus)

	operation := flag.String("op", "exercises", "load,exercises,drop")
	flag.Parse()

	fmt.Println(*operation)
	cfg := Config{
		WorkerCount: cpus * 2,
		User:        "lars",
		Password:    "lars",
		//DbURL:       "127.0.0.1",
		DbURL:     "tdt4225-29.idi.ntnu.no",
		Operation: *operation,
	}
	if err := run(&cfg); err != nil {
		log.Fatalf("Exited with error: %v\n", err)
	}

	fmt.Println("Exited without error")
}

func run(config *Config) error {
	db, err := newDB(config.DbURL, config.User, config.Password, "strava")
	if err != nil {
		return err
	}
	defer db.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	err = db.PingContext(context)
	if err != nil {
		return err
	}

	fmt.Println("Successfully connected to database")

	_, err = os.Stat("./dataset")
	if os.IsNotExist(err) {
		return errors.New("./dataset folder not found")
	}
	if err != nil {
		return err
	}

	userService, err := user.New(db)
	if err != nil {
		return err
	}

	activityService, err := activity.New(db)
	if err != nil {
		return err
	}

	trackpointService, err := trackpoint.New(db)
	if err != nil {
		return err
	}

	switch config.Operation {
	case "load":

		if err := userService.CreateTable(); err != nil {
			return err
		}

		if err := activityService.CreateTable(); err != nil {
			return err
		}

		if err := trackpointService.CreateTable(); err != nil {
			return err
		}

		if err = activityService.LoadStatements(); err != nil {
			return err
		}

		if err = trackpointService.LoadStatements(); err != nil {
			return err
		}

		if err = userService.LoadStatements(); err != nil {
			return err
		}

		err := loadDataset(config, userService, activityService, trackpointService)
		if err != nil {
			return err
		}
	case "exercises":
		if err = activityService.LoadStatements(); err != nil {
			return err
		}

		if err = trackpointService.LoadStatements(); err != nil {
			return err
		}

		if err = userService.LoadStatements(); err != nil {
			return err
		}
		err := runExercises(activityService, trackpointService, userService)
		if err != nil {
			return err
		}
	case "drop":
		_, err = db.Exec("DROP TABLE Trackpoint")
		if err != nil {
			return err
		}
		_, err = db.Exec("DROP TABLE Activity")
		if err != nil {
			return err
		}
		_, err = db.Exec("DROP TABLE User")
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("Invalid operation: " + config.Operation)
	}

	return nil
}

func newDB(host, user, password, dbname string) (*sql.DB, error) {
	dataSource := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?parseTime=true", user, password, host, dbname)
	db, err := sql.Open("mysql", dataSource)
	if err != nil {
		return nil, err
	}

	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
	return db, nil
}

func runExercises(activityService *activity.Service, trackpointService *trackpoint.Service, userService *user.Service) error {
	fmt.Println("------------------")
	fmt.Println("      Task 1      ")
	fmt.Println("------------------")
	if err := task1(activityService, userService, trackpointService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 2      ")
	fmt.Println("------------------")
	if err := task2(activityService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 3      ")
	fmt.Println("------------------")
	if err := task3(activityService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 4      ")
	fmt.Println("------------------")
	if err := task4(userService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 5      ")
	fmt.Println("------------------")
	if err := task5(activityService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 6      ")
	fmt.Println("------------------")
	if err := task6(activityService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 7      ")
	fmt.Println("------------------")
	if err := task7(activityService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 8      ")
	fmt.Println("------------------")
	if err := task8(userService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 9      ")
	fmt.Println("------------------")
	if err := task9(userService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 10      ")
	fmt.Println("------------------")
	if err := task10(userService); err != nil {
		return err
	}

	fmt.Println("------------------")
	fmt.Println("      Task 11      ")
	fmt.Println("------------------")
	if err := task11(activityService); err != nil {
		return err
	}
	return nil
}
