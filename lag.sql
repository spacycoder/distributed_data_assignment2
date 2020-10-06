select `user`, `start`, `stop`, diff from (
    select
    t.*
    , if(@prev_act = @activity_id, TIMESTAMPDIFF(MINUTE, @prev_date, date_time), 0) as diff
    , @prev_date := `start`
    , @prev_act := activity_id
    from
    trackpoint t
    , (select @prev_date := null, @prev_act := null) var_init
    order by activity_id, date_time DESC
) sq
 order by activity_id, date_time


SELECT t.user_id, t.id as tid, t.date_time, t.activity_id as aid FROM Trackpoint t WHERE t.activity_id IS NOT NULL ORDER BY t.activity_id, t.date_time ASC;

SELECT t.user_id, t.id as tid, t.date_time, t.activity_id as aid FROM Trackpoint t WHERE t.activity_id IS NOT NULL ORDER BY t.activity_id, t.date_time ASC;


SELECT user_id, id, date_time, sq.prev_date, activity_id as aid, sq.prev_aid, diff  FROM (
    SELECT 
    t.*,
    @prev_act AS prev_aid,
    @prev_date AS prev_date,
     @prev_act AS previous_op,
    IF(@prev_act = t.activity_id, 1, 0) as diff,
    @prev_date := t.date_time,
    @prev_act := t.activity_id
    FROM Trackpoint t, 
    (SELECT @prev_date := null, @prev_act := null) var_init 
    ORDER BY t.activity_id, t.date_time DESC) sq
WHERE activity_id IS NOT NULL
ORDER BY activity_id, date_time LIMIT 5;



/* SELECT DISTINCT user_id, id, date_time,prev_date,activity_id, previous_act, diff FROM (
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
WHERE activity_id IS NOT NULL AND diff > 4
ORDER BY date_time, activity_id; */

SELECT DISTINCT user_id FROM (
SELECT user_id, id, date_time,prev_date,activity_id, previous_act, diff FROM (
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
WHERE activity_id IS NOT NULL AND diff > 4
ORDER BY date_time, activity_id
) as invalid_activities;

SELECT user_id, SUM(gained) FROM(
    SELECT user_id, altitude,prev_altitude,activity_id, previous_act, gained FROM (
        SELECT
        t.*,
        @prev_act AS previous_act,
        @prev_altitude AS prev_altitude,
        IF(@prev_act = t.activity_id AND t.altitude > @prev_altitude, t.altitude - @prev_altitude, 0) as gained,
        IF(@prev_act<>t.activity_id, @prev_altitude:=9999, @prev_altitude:=t.altitude) as new_altitude,
        @prev_act := t.activity_id
        FROM Trackpoint t INNER JOIN Activity a ON t.activity_id=a.id AND a.transportation_mode='walk' AND t.altitude<>-777,
        (SELECT @prev_altitude := 9999, @prev_act := null) var_init
        ORDER BY t.date_time, t.activity_id) sq
    WHERE activity_id IS NOT NULL
    ORDER BY date_time, activity_id
) as altitude_gained
GROUP BY user_id;
