package trackpoint

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
	"unsafe"
)

func New(db *sql.DB) (*Service, error) {
	// Create index altitude ON Trackpoint(altitude);
	// Create index transportation_mode ON Activity(transportation_mode);
	// Create index activity_id ON Trackpoint (activity_id);

	return &Service{db: db}, nil
}

func (t *Service) CreateTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS Trackpoint (
		id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
		activity_id INT,
		user_id VARCHAR(30),
		lat DOUBLE,
		lon DOUBLE,
		altitude INT,
		date_days DOUBLE,
		date_time DATETIME,
		FOREIGN KEY(activity_id) REFERENCES Activity(id),
		FOREIGN KEY(user_id) REFERENCES User(id),
		INDEX act_date (date_time, activity_id),
		INDEX act (activity_id),
		INDEX coords (lat, lon)
	)`

	_, err := t.db.Exec(query)
	return err
}

type Service struct {
	db                   *sql.DB
	insertTrackpointStmt *sql.Stmt
}

func (t *Service) LoadStatements() error {
	insertTrackpointStmt, err := t.db.PrepareContext(context.TODO(), "INSERT INTO Trackpoint(activity_id, user_id, lat, lon, altitude, date_days, date_time) VALUES( ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	t.insertTrackpointStmt = insertTrackpointStmt
	return nil
}

func (t *Service) GetCount() (int, error) {
	row := t.db.QueryRowContext(context.TODO(), "SELECT COUNT(*) FROM Trackpoint")
	var count int
	row.Scan(&count)
	return count, nil
}

func (t *Service) CreateTrackpoint(activityID *int, userID string, lat, lon float64, altitude int, dateDays float64, datetime time.Time) error {
	_, err := t.insertTrackpointStmt.Exec(activityID, lat, lon, altitude, dateDays, datetime)
	return err
}

func (t *Service) BulkInsertTrackpoint(trackpoints []Trackpoint, numTrackpoints int) error {
	valueArgs := make([]interface{}, numTrackpoints*7, numTrackpoints*7)

	var b strings.Builder
	size := unsafe.Sizeof("(?, ?, ?, ?, ?, ?, ?),")
	b.Grow(int(size) * numTrackpoints)

	fmt.Fprintf(&b, "INSERT INTO Trackpoint(activity_id, user_id, lat, lon, altitude, date_days, date_time) VALUES ")
	for i := 0; i < numTrackpoints; i++ {
		if i == numTrackpoints-1 {
			fmt.Fprintf(&b, "(?, ?, ?, ?, ?, ?, ?)")
		} else {
			fmt.Fprintf(&b, "(?, ?, ?, ?, ?, ?, ?),")

			// alternative:
			// stmt := b.String()
			// stmt = stmt[:len(stmt)-1]
		}

		t := trackpoints[i]
		index := i * 7
		valueArgs[index] = t.ActivityID
		valueArgs[index+1] = t.UserID
		valueArgs[index+2] = t.Lat
		valueArgs[index+3] = t.Lon
		valueArgs[index+4] = t.Altitude
		valueArgs[index+5] = t.DateDays
		valueArgs[index+6] = t.DateTime
	}

	stmt := b.String()
	tx, err := t.db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(stmt, valueArgs...)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (t *Service) Close() {

}
