package keboola

import (
	"github.com/keboola/go-client/pkg/request"
)

// Index of Storage API.
type Index struct {
	Services Services `json:"services"`
	Features Features `json:"features"`
}

// IndexComponents is the Index of Storage API with components included.
type IndexComponents struct {
	Index
	Components Components `json:"components"`
}

// ServiceID is an ID of a Keboola service, for example "encryption".
type ServiceID string

func (u ServiceID) String() string {
	return string(u)
}

// ServiceURL is an url of a Keboola service, for example "https://encryption.keboola.com".
type ServiceURL string

func (u ServiceURL) String() string {
	return string(u)
}

// ServicesMap is immutable map of services, see Services.ToMap.
type ServicesMap struct {
	data map[ServiceID]ServiceURL
}

// Services slice.
type Services []*Service

type Features []string

// FeaturesMap is immutable map of features, see Features.ToMap.
type FeaturesMap struct {
	data map[string]bool
}

// Service is a Keboola service, for example Encryption API.
type Service struct {
	ID  ServiceID  `json:"id"`
	URL ServiceURL `json:"url"`
}

// IndexRequest returns index of Storage API without components definitions.
func (a *API) IndexRequest() request.APIRequest[*Index] {
	index := &Index{}
	req := a.
		newRequest(StorageAPI).
		WithResult(index).
		WithGet("").
		AndQueryParam("exclude", "components")
	return request.NewAPIRequest(index, req)
}

// IndexComponentsRequest returns index of Storage API with components definitions.
func (a *API) IndexComponentsRequest() request.APIRequest[*IndexComponents] {
	result := &IndexComponents{}
	req := a.
		newRequest(StorageAPI).
		WithResult(result).
		WithGet("")
	return request.NewAPIRequest(result, req)
}

// ToMap converts Services slice to ServicesMap.
func (v Services) ToMap() ServicesMap {
	out := ServicesMap{data: make(map[ServiceID]ServiceURL)}
	for _, s := range v {
		out.data[s.ID] = s.URL
	}
	return out
}

// URLByID return service URL by service ID.
func (m ServicesMap) URLByID(serviceID ServiceID) (ServiceURL, bool) {
	v, found := m.data[serviceID]
	return v, found
}

// Len return length of services map.
func (m ServicesMap) Len() int {
	return len(m.data)
}

// ToMap converts Features slice to FeaturesMap.
func (v Features) ToMap() FeaturesMap {
	out := FeaturesMap{data: make(map[string]bool)}
	for _, feature := range v {
		out.data[feature] = true
	}
	return out
}

// Has returns true if project has the feature enabled.
func (m FeaturesMap) Has(feature string) bool {
	return m.data[feature]
}

// AllServices converts services slice to map.
func (i Index) AllServices() ServicesMap {
	return i.Services.ToMap()
}

// ServiceURLByID return service URL by service ID.
func (i Index) ServiceURLByID(serviceID ServiceID) (ServiceURL, bool) {
	return i.AllServices().URLByID(serviceID)
}
