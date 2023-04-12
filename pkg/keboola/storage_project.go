package keboola

import "fmt"

type ProjectID int

func (v ProjectID) String() string {
	if v == 0 {
		panic(fmt.Errorf("projectID cannot be empty"))
	}
	return fmt.Sprintf("%d", v)
}
