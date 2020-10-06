package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/olekukonko/tablewriter"
	"github.com/spacycoder/db_mysql/pkg/activity"
	"github.com/spacycoder/db_mysql/pkg/trackpoint"
	"github.com/spacycoder/db_mysql/pkg/user"
)

func task1(activityService *activity.Service, userService *user.Service, trackpointService *trackpoint.Service) error {
	usersCount, err := userService.GetCount()
	if err != nil {
		return nil
	}

	activityCount, err := activityService.GetCount()
	if err != nil {
		return nil
	}

	trackpointCount, err := trackpointService.GetCount()
	if err != nil {
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Table", "Count"})
	table.Append([]string{
		"User", strconv.Itoa(usersCount),
	})
	table.Append([]string{
		"Actvitity", strconv.Itoa(activityCount),
	})
	table.Append([]string{
		"Trackpoint", strconv.Itoa(trackpointCount),
	})
	table.Render()
	return nil
}

func task2(activityService *activity.Service) error {
	res, err := activityService.AverageActivitesPerUser()
	if err != nil {
		return err
	}

	fmt.Printf("Average number of activities per user is: %f\n", res)
	return nil
}

func task3(activityService *activity.Service) error {
	userIDs, counts, err := activityService.GetUsersActivityCount(20)
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"User Id", "Count"})
	for i, userID := range userIDs {
		table.Append([]string{
			userID,
			strconv.Itoa(counts[i]),
		})
	}
	table.Render()
	return nil
}

func task4(userService *user.Service) error {
	users, err := userService.GetUsersThatHasUsedTransportationMode("Taxi")
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"User ID"})
	for _, u := range users {
		table.Append([]string{
			u,
		})
	}
	table.Render()
	return nil
}

func task5(activityService *activity.Service) error {
	transModes, transCounts, err := activityService.GetTransportationCounts()
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Transportation", "Count"})
	for i, trans := range transModes {
		table.Append([]string{
			trans,
			strconv.Itoa(transCounts[i]),
		})
	}
	table.Render()
	return nil
}

func task6(activityService *activity.Service) error {
	year, count, err := activityService.YearWithMostActivites()
	if err != nil {
		return err
	}

	fmt.Printf("The year %d had %d activities which was the most of any year.\n", year, count)

	yearWithMostHours, _, err := activityService.YearWithMostHours()
	if err != nil {
		return err
	}

	if year == yearWithMostHours {
		fmt.Println("The year with most hours was also the year with most activities.")
	} else {
		fmt.Println("The year with most activities was not the year with most hours.")
	}
	return nil
}

func task7(activityService *activity.Service) error {
	distance, err := activityService.GetDistanceWalkedByUser("112")
	if err != nil {
		return err
	}

	fmt.Printf("Distance: %f\n\n", distance)
	return nil
}

func task8(userService *user.Service) error {
	users, err := userService.GetUsersWithMostAltitude(20)
	if err != nil {
		return err
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"User ID", "Altitude"})
	for _, u := range users {
		table.Append([]string{
			u.UserID,
			strconv.Itoa(u.GainedAltitude) + "m",
		})
	}
	table.Render()
	return nil
}

func task9(userService *user.Service) error {
	users, counts, err := userService.GetUsersWithInvalidActivites()
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"User ID", "Invalid activities"})
	for i, u := range users {
		table.Append([]string{
			u,
			strconv.Itoa(counts[i]),
		})
	}

	table.Render()
	return nil
}

func task10(userService *user.Service) error {
	users, err := userService.UsersInBeijing()
	if err != nil {
		return err
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"User ID"})
	for _, u := range users {
		table.Append([]string{
			u,
		})
	}

	table.Render()
	return nil
}

func task11(activityService *activity.Service) error {
	activities, counts, err := activityService.GetTopTransportationByUsers()
	if err != nil {
		return err
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"User ID", "Mode", "Count"})

	for i, a := range activities {
		table.Append([]string{a.UserID, a.TransportationMode, strconv.Itoa(counts[i])})
	}

	table.Render()
	return nil
}
