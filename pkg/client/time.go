package client

import (
	"strconv"
	"time"
)

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
