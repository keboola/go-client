package storageapi

import (
	"strconv"
	"time"
)

// TimeFormat used in Storage API
const TimeFormat = "2006-01-02T15:04:05-0700"

// Time is encoded/decoded in TimeFormat used in Storage API.
type Time time.Time

// UnmarshalJSON implements JSON decoding.
func (t *Time) UnmarshalJSON(data []byte) (err error) {
	now, err := time.ParseInLocation(`"`+TimeFormat+`"`, string(data), time.Local)
	*t = Time(now)
	return
}

// MarshalJSON implements JSON encoding.
func (t Time) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, len(TimeFormat)+2)
	b = append(b, '"')
	b = time.Time(t).AppendFormat(b, TimeFormat)
	b = append(b, '"')
	return b, nil
}

func (t Time) String() string {
	return time.Time(t).Format(TimeFormat)
}

// DurationSeconds is time.Duration encoded/decoded as number of seconds.
type DurationSeconds time.Duration

// UnmarshalJSON implements JSON decoding.
func (d *DurationSeconds) UnmarshalJSON(data []byte) (err error) {
	v, err := time.ParseDuration(string(data) + "s")
	if err != nil {
		return err
	}
	*d = DurationSeconds(v)
	return
}

// MarshalJSON implements JSON encoding.
func (d DurationSeconds) MarshalJSON() ([]byte, error) {
	return []byte(d.String()), nil
}

func (d DurationSeconds) String() string {
	return strconv.Itoa(int(time.Duration(d).Seconds()))
}
