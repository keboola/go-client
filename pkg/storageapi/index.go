package storageapi

import (
	"sync"

	"github.com/keboola/go-client/pkg/client"
)

// Index of Storage API.
type Index struct {
	Services []*Service `json:"services"`
	Features []string   `json:"features"`

	lock        *sync.Mutex
	servicesMap Services
}

// IndexComponents is Index of Storage API with components included.
type IndexComponents struct {
	Index
	Components Components `json:"components"`
}

// Services slice.
type Services map[ServiceID]ServiceURL

// ServiceID is id of a Keboola service, for example "encryption".
type ServiceID string

func (u ServiceID) String() string {
	return string(u)
}

// ServiceURL is url of a Keboola service, for example "https://encryption.keboola.com".
type ServiceURL string

func (u ServiceURL) String() string {
	return string(u)
}

// Service is a Keboola service, for example Encryption API.
type Service struct {
	ID  ServiceID  `json:"id"`
	URL ServiceURL `json:"url"`
}

// URLByID return service URL by service ID.
func (s Services) URLByID(serviceID ServiceID) (ServiceURL, bool) {
	v, found := s[serviceID]
	return v, found
}

// IndexRequest returns index of Storage API without components definitions.
func IndexRequest() client.APIRequest[*Index] {
	index := &Index{lock: &sync.Mutex{}}
	request := newRequest().
		WithResult(index).
		WithGet("").
		AndQueryParam("exclude", "components")
	return client.NewAPIRequest(index, request)
}

// IndexComponentsRequest returns index of Storage API with components definitions.
func IndexComponentsRequest() client.APIRequest[*IndexComponents] {
	result := &IndexComponents{Index: Index{lock: &sync.Mutex{}}}
	request := newRequest().
		WithResult(result).
		WithGet("")
	return client.NewAPIRequest(result, request)
}

// AllServices converts services slice to map.
func (i Index) AllServices() Services {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.servicesMap == nil {
		i.servicesMap = make(Services)
		for _, s := range i.Services {
			i.servicesMap[s.ID] = s.URL
		}
	}
	return i.servicesMap
}

// ServiceURLByID return service URL by service ID.
func (i Index) ServiceURLByID(serviceID ServiceID) (ServiceURL, bool) {
	return i.AllServices().URLByID(serviceID)
}
