package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spacycoder/db_mysql/pkg/activity"
	"github.com/spacycoder/db_mysql/pkg/trackpoint"
	"github.com/spacycoder/db_mysql/pkg/user"
)

func worker(tracker chan empty, users chan user.User, activityService *activity.Service, trackpointService *trackpoint.Service) {
	trackpoints := make([]trackpoint.Trackpoint, 2500, 2500)
	activities := make([]activity.Activity, 100, 100)

	for u := range users {
		if u.HasLabels {
			err := createActivities(u, activityService, activities)
			if err != nil {
				panic(err)
			}
		}

		path := fmt.Sprintf("./dataset/Data/%s/Trajectory", u.ID)
		files, err := ioutil.ReadDir(path)
		if err != nil {
			panic(err)
		}

		for _, file := range files {
			err := createTrajectories(u, file.Name(), activityService, trackpointService, trackpoints)
			if err != nil {
				panic(err)
			}
		}
	}

	var e empty
	tracker <- e
}

func loadDataset(config *Config, userService *user.Service, activityService *activity.Service, trackpointService *trackpoint.Service) error {
	fmt.Println("Loading dataset")

	insertUsers(userService)
	usersChan := make(chan user.User, config.WorkerCount)
	tracker := make(chan empty)

	users, err := userService.GetUsers()
	if err != nil {
		return err
	}

	startTime := time.Now()
	// start workers
	for i := 0; i < config.WorkerCount; i++ {
		go worker(tracker, usersChan, activityService, trackpointService)
	}

	// push users to workers
	for _, u := range users {
		usersChan <- u
	}
	close(usersChan)

	// wait for workers to finish
	for i := 0; i < config.WorkerCount; i++ {
		<-tracker
	}

	fmt.Printf("Finished in %s\n", time.Since(startTime))
	return nil
}

func insertUsers(userService *user.Service) error {
	f, err := os.Open("./dataset/labeled_ids.txt")
	if err != nil {
		return err
	}
	scanner := bufio.NewScanner(f)
	labeledUsers := make(map[string]struct{})

	for scanner.Scan() {
		user := scanner.Text()
		if strings.TrimSpace(user) == "" {
			continue
		}

		labeledUsers[user] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	files, err := ioutil.ReadDir("./dataset/Data/")
	if err != nil {
		return err
	}
	for _, file := range files {
		_, exists := labeledUsers[file.Name()]
		err := userService.CreateUser(file.Name(), exists)
		if err != nil {
			return err
		}
	}

	return nil
}

func createTrajectories(user user.User, fileName string, activityService *activity.Service, trackpointService *trackpoint.Service, trackpoints []trackpoint.Trackpoint) error {
	filePath := fmt.Sprintf("./dataset/Data/%s/Trajectory/%s", user.ID, fileName)
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	valid, err := isValidLineCount(f)
	if err != nil {
		return err
	}
	if !valid {
		return nil
	}
	f.Seek(0, io.SeekStart)
	scanner := bufio.NewScanner(f)
	// skip first 6 lines
	scanner.Scan()
	scanner.Scan()
	scanner.Scan()
	scanner.Scan()
	scanner.Scan()
	scanner.Scan()
	var activities []activity.Activity
	if user.HasLabels {
		activities, err = activityService.GetActivitiesForUser(user.ID)
		if err != nil {
			return err
		}
	}

	currActivityIndex := 0
	trackpointIndex := 0

	for scanner.Scan() {
		row := scanner.Text()
		row = strings.TrimSpace(row)
		if row == "" {
			continue
		}

		cols := strings.Split(row, ",")
		layout := "2006-01-02T15:04:05"

		date, err := time.Parse(layout, cols[5]+"T"+cols[6])
		if err != nil {
			return err
		}
		var activityID *int
		offset := 0
		if len(activities) > 0 && currActivityIndex < len(activities) {
			offset, activityID = getActivityIDForTrackpoint(date, activities[currActivityIndex:])
			currActivityIndex += offset
		}
		lat, err := strconv.ParseFloat(cols[0], 64)
		if err != nil {
			return err
		}
		lon, err := strconv.ParseFloat(cols[1], 64)
		if err != nil {
			return err
		}
		alt, err := strconv.ParseFloat(cols[3], 64) // alt is in some cases float
		if err != nil {
			return err
		}
		days, err := strconv.ParseFloat(cols[4], 64)
		if err != nil {
			return err
		}

		trackpoints[trackpointIndex].Altitude = int(alt)
		trackpoints[trackpointIndex].UserID = user.ID
		trackpoints[trackpointIndex].Lat = lat
		trackpoints[trackpointIndex].Lon = lon
		trackpoints[trackpointIndex].DateTime = date
		trackpoints[trackpointIndex].DateDays = days
		trackpoints[trackpointIndex].ActivityID = activityID
		trackpointIndex++
	}

	if trackpointIndex > 0 {
		return trackpointService.BulkInsertTrackpoint(trackpoints, trackpointIndex)
	}

	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func createActivities(user user.User, activityService *activity.Service, activities []activity.Activity) error {
	activityPath := fmt.Sprintf("./dataset/Data/%s/labels.txt", user.ID)
	f, err := os.Open(activityPath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	activityIndex := 0
	// skip first line
	scanner.Scan()
	for scanner.Scan() {
		row := scanner.Text()
		row = strings.TrimSpace(row)
		if row == "" {
			continue
		}
		cols := strings.Split(row, "\t")

		startDate, err := time.Parse(dateLayout, cols[0])
		if err != nil {
			return err
		}
		endDate, err := time.Parse(dateLayout, cols[1])
		if err != nil {
			return err
		}

		activities[activityIndex].UserID = user.ID
		activities[activityIndex].TransportationMode = cols[2]
		activities[activityIndex].StartDateTime = startDate
		activities[activityIndex].EndDateTime = endDate
		activityIndex++

		if activityIndex >= len(activities) {
			activityService.BulkCreateActivity(activities, activityIndex)
			activityIndex = 0
		}
	}

	if activityIndex > 0 {
		activityService.BulkCreateActivity(activities, activityIndex)
		activityIndex = 0
	}
	return nil
}

func isValidLineCount(r io.Reader) (bool, error) {
	buf := make([]byte, 1024*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)
		if count > validLineCount {
			return false, nil
		}
		switch {
		case err == io.EOF:
			return true, nil

		case err != nil:
			return false, err
		}
	}
}

func getActivityIDForTrackpoint(timestamp time.Time, activities []activity.Activity) (int, *int) {
	indexOffset := 0
	if activities[indexOffset].StartDateTime.After(timestamp) {
		return 0, nil
	}
	if activities[indexOffset].EndDateTime.After(timestamp) {
		return 0, &activities[indexOffset].ID
	}

	indexOffset++
	if indexOffset >= len(activities) {
		return indexOffset, nil
	}
	if activities[indexOffset].StartDateTime.After(timestamp) {
		return indexOffset, nil
	}

	return indexOffset, &activities[indexOffset].ID
}
