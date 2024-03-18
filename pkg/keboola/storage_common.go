package keboola

import "sort"

type Object interface {
	ObjectID() any
}

// Metadata - object metadata.
type Metadata map[string]string

type MetadataKV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type MetadataPayload struct {
	Metadata []MetadataKV `json:"metadata"`
}

// MetadataDetails - metadata with details (id, timestamp).
type MetadataDetails []MetadataDetail

// MetadataDetail - metadata with details (id, timestamp).
type MetadataDetail struct {
	ID        string `json:"id"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Timestamp string `json:"timestamp"`
	Provider  string `json:"provider"`
}

// ToMap converts slice to map.
func (v MetadataDetails) ToMap() Metadata {
	out := make(Metadata)
	for _, item := range v {
		out[item.Key] = item.Value
	}
	return out
}

func (m Metadata) ToPayload() (payload MetadataPayload) {
	for k, v := range m {
		payload.Metadata = append(payload.Metadata, MetadataKV{Key: k, Value: v})
	}
	sort.SliceStable(payload.Metadata, func(i, j int) bool {
		return payload.Metadata[i].Key < payload.Metadata[j].Key
	})
	return payload
}

// DeleteOption for requests to delete bucket or table.
type DeleteOption func(c *deleteConfig)

type deleteConfig struct {
	force bool
}

func WithForce() DeleteOption {
	return func(c *deleteConfig) {
		c.force = true
	}
}
