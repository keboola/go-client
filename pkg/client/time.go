package client

import (
	"strconv"
	"time"
)

// Time is encoded/decoded in TimeFormat with format RFC3339 used in Keboola APIs.
type Time time.Time

// UnmarshalJSON implements JSON decoding.
func (t *Time) UnmarshalJSON(data []byte) (err error) {
	now, _ := time.ParseInLocation(`"`+time.RFC3339+`"`, string(data), time.Local)
	*t = Time(now)
	return
}

// MarshalJSON implements JSON encoding.
func (t Time) MarshalJSON() ([]byte, error) {
	b := make([]byte, 0, len(time.RFC3339)+2)
	b = append(b, '"')
	b = time.Time(t).AppendFormat(b, time.RFC3339)
	b = append(b, '"')
	return b, nil
}

func (t Time) String() string {
	return time.Time(t).Format(time.RFC3339)
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
