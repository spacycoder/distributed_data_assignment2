package activity

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"
	"unsafe"
)

func New(db *sql.DB) (*Service, error) {
	/* _, err = a.db.ExecContext(context.TODO(), "CREATE VIEW ActivitiesPerYear AS SELECT YEAR(start_date_time) as year, COUNT(*) AS count FROM Activity GROUP BY YEAR(start_date_time) ORDER BY count DESC")
	if err != nil {
		return nil, err
	} */

	return &Service{db: db}, nil
}

type Service struct {
	db                           *sql.DB
	insertActivityStmt           *sql.Stmt
	queryActivityIDWithTimeStamp *sql.Stmt
	queryActivitiesForUser       *sql.Stmt
	queryAverageActivites        *sql.Stmt
}

func (a *Service) LoadStatements() error {
	insertActivityStmt, err := a.db.PrepareContext(context.TODO(), "INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES( ?, ?, ?, ? )")
	if err != nil {
		return err
	}
	queryActivityIDWithTimeStamp, err := a.db.PrepareContext(context.TODO(), "SELECT id FROM Activity WHERE ? BETWEEN start_date_time AND end_date_time")
	if err != nil {
		return err
	}
	queryActivitiesForUser, err := a.db.PrepareContext(context.TODO(), "SELECT * FROM Activity WHERE user_id = ? ORDER BY start_date_time ASC")
	if err != nil {
		return err
	}
	queryAverageActivites, err := a.db.PrepareContext(context.TODO(), "SELECT AVG(count) FROM UserActivityCount")
	if err != nil {
		return err
	}
	a.insertActivityStmt = insertActivityStmt
	a.queryActivityIDWithTimeStamp = queryActivityIDWithTimeStamp
	a.queryActivitiesForUser = queryActivitiesForUser
	a.queryAverageActivites = queryAverageActivites
	return nil
}

func (a *Service) CreateTable() error {
	query := `
	CREATE TABLE IF NOT EXISTS Activity (
		id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
		user_id VARCHAR(30),
		transportation_mode VARCHAR(30),
		start_date_time DATETIME,
		end_date_time DATETIME,
		FOREIGN KEY (user_id) REFERENCES User(id),
		INDEX tran_user (transportation_mode, user_id)
	)`

	_, err := a.db.Exec(query)
	if err != nil {
		return err
	}
	_, err = a.db.ExecContext(context.TODO(), "CREATE OR REPLACE VIEW UserActivityCount AS SELECT user_id, COUNT(*) as count FROM Activity GROUP BY user_id")
	if err != nil {
		return err
	}
	return err
}

func (a *Service) CreateActivity(userID, transportationMode string, startDateTime, endDateTime time.Time) error {
	_, err := a.insertActivityStmt.ExecContext(context.TODO(), userID, transportationMode, startDateTime, endDateTime)
	return err
}

func (a *Service) BulkCreateActivity(activities []Activity, numActivities int) error {
	valueArgs := make([]interface{}, numActivities*4, numActivities*4)

	var b strings.Builder
	size := unsafe.Sizeof("(?, ?, ?, ?),")
	b.Grow(int(size) * numActivities)

	fmt.Fprintf(&b, "INSERT INTO Activity(user_id, transportation_mode, start_date_time, end_date_time) VALUES ")
	for i := 0; i < numActivities; i++ {
		if i == numActivities-1 {
			fmt.Fprintf(&b, "(?, ?, ?, ?)")
		} else {
			fmt.Fprintf(&b, "(?, ?, ?, ?),")

			// alternative:
			// stmt := b.String()
			// stmt = stmt[:len(stmt)-1]
		}

		a := activities[i]
		index := i * 4
		valueArgs[index] = a.UserID
		valueArgs[index+1] = a.TransportationMode
		valueArgs[index+2] = a.StartDateTime
		valueArgs[index+3] = a.EndDateTime
	}

	stmt := b.String()
	tx, err := a.db.Begin()
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

func (a *Service) GetActivitiesForUser(userID string) ([]Activity, error) {
	rows, err := a.queryActivitiesForUser.Query(userID)
	if err != nil {
		return nil, err
	}
	var activities []Activity

	var id int
	var uID string
	var transportationMode string
	var startDateTime time.Time
	var endDateTime time.Time

	for rows.Next() {
		rows.Scan(&id, &uID, &transportationMode, &startDateTime, &endDateTime)
		activities = append(activities, Activity{
			ID: id, UserID: uID, TransportationMode: transportationMode, StartDateTime: startDateTime, EndDateTime: endDateTime,
		})
	}
	return activities, nil
}

func (a *Service) AverageActivitesPerUser() (float64, error) {
	var avg float64
	err := a.queryAverageActivites.QueryRow().Scan(&avg)
	return avg, err
}

func (a *Service) YearWithMostActivites() (int, int, error) {
	query := "SELECT YEAR(start_date_time) as year, COUNT(*) AS count FROM Activity GROUP BY YEAR(start_date_time) ORDER BY count DESC LIMIT 1"
	var count int
	var year int
	row := a.db.QueryRowContext(context.TODO(), query)
	err := row.Scan(&year, &count)
	return year, count, err
}

func (a *Service) YearWithMostHours() (int, int, error) {
	query := "SELECT YEAR(start_date_time) as year, SUM(TIMESTAMPDIFF(hour,start_date_time, end_date_time)) duration FROM Activity GROUP BY YEAR(start_date_time) ORDER BY duration DESC LIMIT 1"
	var hours int
	var year int
	row := a.db.QueryRowContext(context.TODO(), query)
	err := row.Scan(&year, &hours)
	return year, hours, err
}

func (a *Service) GetActivityIDForUserWithTimestamp(userID string, timeStamp time.Time) (*int, error) {
	rows, err := a.queryActivityIDWithTimeStamp.Query(timeStamp)
	if err != nil {
		return nil, err
	}

	var id *int

	for rows.Next() {
		rows.Scan(&id)
		return id, nil
	}

	return nil, nil
}

func (a *Service) GetActivity() {

}

func (a *Service) Close() {
	// a.insertActivityStmt.Close()
}

func (t *Service) GetCount() (int, error) {
	row := t.db.QueryRowContext(context.TODO(), "SELECT COUNT(*) FROM Activity")
	var count int
	row.Scan(&count)
	return count, nil
}

// GetUsersActivityCount
func (a *Service) GetUsersActivityCount(limit int) ([]string, []int, error) {
	var rows *sql.Rows
	var err error
	var uIds []string
	var counts []int
	if limit == -1 {
		rows, err = a.db.QueryContext(context.TODO(), "SELECT user_id, COUNT(user_id) FROM Activity GROUP BY user_id ORDER BY 2 DESC")
		if err != nil {
			return nil, nil, err
		}
	} else {
		rows, err = a.db.QueryContext(context.TODO(), "SELECT user_id, COUNT(user_id) FROM Activity GROUP BY user_id ORDER BY 2 DESC LIMIT ?", limit)
		if err != nil {
			return nil, nil, err
		}
	}

	var uID string
	var count int

	for rows.Next() {
		rows.Scan(&uID, &count)
		uIds = append(uIds, uID)
		counts = append(counts, count)
	}
	return uIds, counts, nil
}

func (a *Service) GetTopTransportationByUsers() ([]Activity, []int, error) {
	rows, err := a.db.QueryContext(context.TODO(), `SELECT u.id, a.transportation_mode, COUNT(a.transportation_mode) as ActivityCount 
		FROM User as u INNER JOIN Activity as a 
		ON u.id=a.user_id 
		GROUP BY u.id, a.transportation_mode 
		ORDER BY u.id, ActivityCount DESC`)
	if err != nil {
		return nil, nil, err
	}

	var activities []Activity
	var counts []int

	var previousUser string

	for rows.Next() {
		var activity Activity
		var count int
		rows.Scan(&activity.UserID, &activity.TransportationMode, &count)

		if previousUser == activity.UserID {
			continue
		}

		previousUser = activity.UserID
		activities = append(activities, activity)
		counts = append(counts, count)
	}

	return activities, counts, nil
}

func (a *Service) GetTransportationCounts() ([]string, []int, error) {
	rows, err := a.db.QueryContext(context.TODO(), "SELECT transportation_mode, COUNT(transportation_mode) FROM Activity GROUP BY transportation_mode ORDER BY 2 DESC")
	if err != nil {
		return nil, nil, err
	}
	var transMode string
	var count int

	var transportationModes []string
	var counts []int

	for rows.Next() {
		rows.Scan(&transMode, &count)
		transportationModes = append(transportationModes, transMode)
		counts = append(counts, count)
	}

	return transportationModes, counts, nil
}

func (a *Service) GetDistanceWalkedByUser(userId string) (float64, error) {
	activityRows, err := a.db.QueryContext(context.TODO(), `SELECT a.id, a.transportation_mode, t.lat, t.lon, t.date_time as date FROM Activity as a 
		INNER JOIN Trackpoint as t ON a.id=t.activity_id 
		AND a.user_id = ? 
		AND a.transportation_mode = 'walk' 
		AND YEAR(a.start_date_time) = '2008' 
		ORDER BY t.date_time`, userId)
	if err != nil {
		return 0, err
	}

	var transMode string
	var lat float64
	var lon float64
	var date time.Time
	var activityID string

	prevLat := -999.0
	prevLon := -999.0

	distance := 0.0
	var currentActivityID string

	for activityRows.Next() {
		activityRows.Scan(&activityID, &transMode, &lat, &lon, &date)

		if activityID != currentActivityID || prevLat == -999.0 {
			prevLat = lat
			prevLon = lon
			currentActivityID = activityID
			continue
		}
		distance += calculateDistance(prevLat, prevLon, lat, lon)
		prevLat = lat
		prevLon = lon
	}

	return distance, nil
}

func calculateDistance(fromLat float64, fromLon float64, toLat float64, toLon float64) float64 {
	lat1 := fromLat * math.Pi / 180.0
	lon1 := fromLon * math.Pi / 180.0
	lat2 := toLat * math.Pi / 180.0
	lon2 := toLon * math.Pi / 180.0

	diffLat := lat2 - lat1
	diffLon := lon2 - lon1

	ans := math.Pow(math.Sin(diffLat/2.0), 2) + (math.Cos(lat1) * math.Cos(lat2) * math.Pow(math.Sin(diffLon/2.0), 2))
	ans = 2.0 * math.Asin(math.Sqrt(ans))

	earthRadius := 6371.0

	return ans * earthRadius
}
