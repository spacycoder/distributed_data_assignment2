package trackpoint

import "time"

type Trackpoint struct {
	ID         int
	UserID     string
	ActivityID *int
	Lat        float64
	Lon        float64
	Altitude   int
	DateDays   float64
	DateTime   time.Time
}
