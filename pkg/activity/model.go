package activity

import (
	"time"
)

type ActivityMode string

const (
	BUS  ActivityMode = "bus"
	BIKE ActivityMode = "bike"
)

type Activity struct {
	ID                 int
	UserID             string
	TransportationMode string
	StartDateTime      time.Time
	EndDateTime        time.Time
}

type SortByDate []Activity

func (a SortByDate) Len() int      { return len(a) }
func (a SortByDate) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a SortByDate) Less(i, j int) bool {
	return a[i].StartDateTime.Before(a[j].StartDateTime)
}
