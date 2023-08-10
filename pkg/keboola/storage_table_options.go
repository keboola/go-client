package keboola

import "strings"

// CreateTableOption applies to the request for creating table from file.
type CreateTableOption interface {
	applyCreateTableOption(c *createTableConfig)
}

// LoadDataOption applies to the request loading data to a table.
type LoadDataOption interface {
	applyLoadDataOption(c *loadDataConfig)
}

// loadDataFromFileConfig contains common params to load data from file resource.
type loadDataFromFileConfig struct {
	Delimiter string `json:"delimiter,omitempty" writeoptional:"true"`
	Enclosure string `json:"enclosure,omitempty" writeoptional:"true"`
	EscapedBy string `json:"escapedBy,omitempty" writeoptional:"true"`
}

// createTableConfig contains params to create table from file resource.
type createTableConfig struct {
	loadDataFromFileConfig
	PrimaryKey string `json:"primaryKey,omitempty" writeoptional:"true"`
}

// loadDataConfig contains params to load data to a table from file resource.
type loadDataConfig struct {
	loadDataFromFileConfig
	IncrementalLoad int      `json:"incremental,omitempty" writeoptional:"true"`
	WithoutHeaders  int      `json:"withoutHeaders,omitempty" writeoptional:"true"`
	Columns         []string `json:"columns,omitempty" writeoptional:"true"`
}

type (
	// delimiterOption specifies field delimiter used in the CSV file. Default value is ','.
	delimiterOption string

	// enclosureOption specifies field enclosure used in the CSV file. Default value is '"'.
	enclosureOption string

	// escapedByOption specifies escape character used in the CSV file. The default value is an empty value - no escape character is used.
	// Note: you can specify either enclosure or escapedBy parameter, not both.
	escapedByOption string

	// primaryKeyOption specifies primary key of the table. Multiple columns can be separated by a comma.
	primaryKeyOption string

	// incrementalLoadOption decides whether the target table will be truncated before import.
	incrementalLoadOption bool

	// columnsHeadersOption specifies list of columns present in the CSV file.
	// The first line of the file will not be treated as a header.
	columnsHeadersOption []string

	// withoutHeaderOption specifies if the csv file contains header. If it doesn't, columns are matched by their order.
	// If this option is used, columns option is ignored.
	withoutHeaderOption bool
)

func WithDelimiter(d string) delimiterOption {
	return delimiterOption(d)
}

func WithEnclosure(e string) enclosureOption {
	return enclosureOption(e)
}

func WithEscapedBy(e string) escapedByOption {
	return escapedByOption(e)
}

func WithPrimaryKey(pk []string) primaryKeyOption {
	return primaryKeyOption(strings.Join(pk, ","))
}

func WithIncrementalLoad(i bool) incrementalLoadOption {
	return incrementalLoadOption(i)
}

func WithColumnsHeaders(c []string) columnsHeadersOption {
	return c
}

func WithoutHeader(h bool) withoutHeaderOption {
	return withoutHeaderOption(h)
}

func (o delimiterOption) applyCreateTableOption(c *createTableConfig) {
	c.Delimiter = string(o)
}

func (o delimiterOption) applyLoadDataOption(c *loadDataConfig) {
	c.Delimiter = string(o)
}

func (o enclosureOption) applyCreateTableOption(c *createTableConfig) {
	c.Enclosure = string(o)
}

func (o enclosureOption) applyLoadDataOption(c *loadDataConfig) {
	c.Enclosure = string(o)
}

func (o escapedByOption) applyCreateTableOption(c *createTableConfig) {
	c.EscapedBy = string(o)
}

func (o escapedByOption) applyLoadDataOption(c *loadDataConfig) {
	c.EscapedBy = string(o)
}

func (o primaryKeyOption) applyCreateTableOption(c *createTableConfig) {
	c.PrimaryKey = string(o)
}

func (o incrementalLoadOption) applyLoadDataOption(c *loadDataConfig) {
	c.IncrementalLoad = 0
	if o {
		c.IncrementalLoad = 1
	}
}

func (o columnsHeadersOption) applyLoadDataOption(c *loadDataConfig) {
	c.Columns = o
}

func (o withoutHeaderOption) applyLoadDataOption(c *loadDataConfig) {
	c.WithoutHeaders = 0
	if o {
		c.WithoutHeaders = 1
	}
}
