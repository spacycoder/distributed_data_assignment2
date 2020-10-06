package user

import (
	"context"
	"database/sql"
	"sort"
	"sync"
)

func New(db *sql.DB) (*Service, error) {
	return &Service{db: db}, nil
}

func (u *Service) LoadStatements() error {
	userInsertStmt, err := u.db.PrepareContext(context.TODO(), "INSERT INTO User(id, has_labels) VALUES( ?, ? )")
	if err != nil {
		return err
	}
	u.userInsertStmt = userInsertStmt
	return nil
}

func (u *Service) CreateTable() error {
	query := `
		CREATE TABLE IF NOT EXISTS User (
			id VARCHAR(30) NOT NULL PRIMARY KEY,
			has_labels BOOL
		)
	`

	_, err := u.db.Exec(query)
	return err
}

type Service struct {
	db             *sql.DB
	userInsertStmt *sql.Stmt
}

func (u *Service) GetUsers() ([]User, error) {
	var users []User
	rows, err := u.db.QueryContext(context.TODO(), "SELECT * FROM User")
	if err != nil {
		return nil, err
	}

	var user string
	var hasLabel bool

	for rows.Next() {
		rows.Scan(&user, &hasLabel)
		users = append(users, User{
			ID:        user,
			HasLabels: hasLabel,
		})
	}

	return users, nil
}

func (u *Service) GetUsersThatHasUsedTransportationMode(transportationMode string) ([]string, error) {
	queryTransportation, err := u.db.PrepareContext(context.TODO(), "SELECT DISTINCT a.user_id FROM Activity a WHERE a.transportation_mode = (?)")
	if err != nil {
		return nil, err
	}
	rows, err := queryTransportation.QueryContext(context.TODO(), transportationMode)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	users := []string{}
	for rows.Next() {
		var user string
		if err := rows.Scan(&user); err != nil {
			return nil, err
		}

		users = append(users, user)
	}

	return users, err
}

type UserWithAltitude struct {
	UserID         string
	GainedAltitude int
}

func (u *Service) GetCount() (int, error) {
	row := u.db.QueryRowContext(context.TODO(), "SELECT COUNT(*) FROM User")
	var count int
	row.Scan(&count)
	return count, nil
}

func (u *Service) GetUsersWithActivities() ([]User, error) {
	query := `SELECT * FROM User WHERE has_labels = true`
	stmt, err := u.db.PrepareContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.QueryContext(context.TODO())
	if err != nil {
		return nil, err
	}
	users := []User{}
	for rows.Next() {
		var user User
		rows.Scan(&user.ID, &user.HasLabels)
		users = append(users, user)
	}

	return users, nil
}

func (u *Service) GetUsersWithMostAltitude(numUsers int) ([]UserWithAltitude, error) {
	users, err := u.GetUsers()
	var wg sync.WaitGroup

	query := `SELECT t.altitude, t.activity_id FROM Trackpoint t INNER JOIN Activity a ON t.activity_id=a.id AND a.transportation_mode="Walk" AND t.altitude<>-777 AND a.user_id=? ORDER BY t.date_time, t.activity_id`
	stmt, err := u.db.PrepareContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}

	usersWithAltitude := make([]UserWithAltitude, len(users), len(users))
	for i, u := range users {
		wg.Add(1)
		go func(u User, index int) {
			rows, err := stmt.QueryContext(context.TODO(), u.ID)
			if err != nil {
				panic(err)
			}
			gainedAltitude := 0
			prevAltitude := 0
			currentActivityId := -1
			for rows.Next() {
				var altitude int
				var id int
				rows.Scan(&altitude, &id)
				if id != currentActivityId {
					currentActivityId = id
					prevAltitude = altitude
					continue
				}
				if altitude > prevAltitude {
					gainedAltitude += altitude - prevAltitude
				}
				prevAltitude = altitude
			}

			userWithAlt := UserWithAltitude{
				UserID:         u.ID,
				GainedAltitude: gainedAltitude,
			}
			usersWithAltitude[index] = userWithAlt
			wg.Done()
		}(u, i)
	}

	wg.Wait()
	sort.Slice(usersWithAltitude, func(i, j int) bool {
		return usersWithAltitude[i].GainedAltitude > usersWithAltitude[j].GainedAltitude
	})
	if len(usersWithAltitude) > numUsers {
		usersWithAltitude = usersWithAltitude[0:numUsers]
	}
	return usersWithAltitude, nil
}

func (u *Service) UsersInBeijing() ([]string, error) {
	query := "SELECT DISTINCT user_id FROM Trackpoint WHERE ABS(lat-39.916)<=0.001 AND ABS(lon-116.397)<=0.001 GROUP BY Trackpoint.user_id;"
	stmt, err := u.db.PrepareContext(context.TODO(), query)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.QueryContext(context.TODO())
	if err != nil {
		return nil, err
	}
	users := []string{}
	for rows.Next() {
		var user string
		rows.Scan(&user)
		users = append(users, user)
	}

	return users, nil
}

func (u *Service) CreateUser(id string, hasLabels bool) error {
	_, err := u.userInsertStmt.ExecContext(context.TODO(), id, hasLabels)
	return err
}

func (u *Service) GetUser(id string) {

}

func (u *Service) GetUsersWithInvalidActivites() ([]string, []int, error) {
	query := `SELECT invalid.user_id, COUNT(*) FROM (SELECT DISTINCT user_id, activity_id FROM (
		SELECT
		t.*,
		@prev_act AS previous_act,
		@prev_date AS prev_date,
		IF(@prev_act = t.activity_id, ABS(TIMESTAMPDIFF(MINUTE, @prev_date, t.date_time)), 4) as diff,
		@prev_date := t.date_time,
		@prev_act := t.activity_id
		FROM Trackpoint t,
		(SELECT @prev_date := null, @prev_act := null) var_init
		ORDER BY t.date_time, t.activity_id) sq
		WHERE activity_id IS NOT NULL AND diff > 4) AS invalid
	GROUP BY invalid.user_id`

	stmt, err := u.db.PrepareContext(context.TODO(), query)
	if err != nil {
		return nil, nil, err
	}

	rows, err := stmt.QueryContext(context.TODO())
	if err != nil {
		return nil, nil, err
	}
	users := []string{}
	counts := []int{}
	for rows.Next() {
		var user string
		var count int
		rows.Scan(&user, &count)

		users = append(users, user)
		counts = append(counts, count)
	}

	return users, counts, nil
}
