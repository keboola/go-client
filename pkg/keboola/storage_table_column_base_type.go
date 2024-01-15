package keboola

type BaseType string

const (
	TypeInt       BaseType = "INTEGER"
	TypeBoolean   BaseType = "BOOLEAN"
	TypeDate      BaseType = "DATE"
	TypeFloat     BaseType = "FLOAT"
	TypeNumeric   BaseType = "NUMERIC"
	TypeString    BaseType = "STRING"
	TypeTimestamp BaseType = "TIMESTAMP"
)

func (bt BaseType) String() string {
	return string(bt)
}
