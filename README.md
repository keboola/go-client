# Keboola Go Client

## Packages

### `request` package

The `request` package provides abstract and immutable definition of an HTTP request by the `request.HTTPRequest`.

One or more HTTP requests can be grouped together to the generic type `request.APIRequest[R]`,
where the `R` is a result type to which HTTP requests are mapped.

Request sending is provided by the `Sender` interface, the `client` package provides its default implementation.

### `client` package

The `client` package provides default implementation of the `request.Sender` interface based on the standard `net/http` package.

### `keboola` package

The `keboola` package provides the `keboola.API` implementation, it covers:
  - [Storage API](https://keboola.docs.apiary.io/#)
  - [Encryption API](https://keboolaencryption.docs.apiary.io/#)
  - [Jobs Queue API](https://app.swaggerhub.com/apis-docs/keboola/job-queue-api)
  - [Sandboxes API](https://sandboxes.keboola.com/documentation)
  - [Scheduler API](https://app.swaggerhub.com/apis/odinuv/scheduler)

Not all API requests are covered, API requests are extended as needed.

## Quick Start

```go
ctx := context.TODO()

// Create API instance
api, err := keboola.NewAPI(
  ctx, 
  "https://connection.keboola.com", 
  keboola.WithTracerProvider(tracerProvider), 
  keboola.WithMeterProvider(meterProvider),
)
if err != nil {
  return err
}

// Send a request
config, err := api.CreateConfigRequest(&keboola.ConfigWithRows{/*...*/}).Send(ctx)
if err != nil {
  return err
}
```

## Direct HTTP Requests

The `request` package provides a flexible way to make direct HTTP requests using the `NewHTTPRequest` function with any implementation of the `Sender` interface. This approach offers several advantages:

- **Immutability**: Each method call returns a new request instance, allowing for safe request modification.
- **Fluent API**: Chain method calls for a clean and readable request definition.
- **Type Safety**: Response mapping to Go structs with automatic JSON unmarshaling.
- **Error Handling**: Custom error types for structured error responses.
- **Middleware Support**: Callbacks for request/response processing.

The following example demonstrates how to use `NewHTTPRequest` with the default `client.Client` implementation:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/request"
)

// APIError represents an API error response
type APIError struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// Error implements the error interface
func (e APIError) Error() string {
	return fmt.Sprintf("API error %d: %s", e.Code, e.Message)
}

func main() {
	// Create a context
	ctx := context.Background()

	// Create a client (implements the request.Sender interface)
	c := client.New().
		WithBaseURL("https://api.example.com").
		WithHeader("Authorization", "Bearer your-token-here")

	// Define a struct for the response
	type User struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	// Create and send a GET request
	var user User
	_, _, err := request.NewHTTPRequest(c).
		WithGet("/users/123").
		WithResult(&user).
		Send(ctx)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Printf("User: %+v\n", user)

	// Create and send a POST request with JSON body
	newUser := User{Name: "John Doe"}
	var createdUser User
	_, _, err = request.NewHTTPRequest(c).
		WithPost("/users").
		WithJSONBody(newUser).
		WithResult(&createdUser).
		Send(ctx)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Printf("Created user: %+v\n", createdUser)

	// Handle errors with custom error type
	var result User
	_, _, err = request.NewHTTPRequest(c).
		WithGet("/users/999").
		WithResult(&result).
		WithError(&APIError{}).
		Send(ctx)
	if err != nil {
		fmt.Printf("Error handled: %v\n", err)
	}

	// Using query parameters
	var users []User
	_, _, err = request.NewHTTPRequest(c).
		WithGet("/users").
		AndQueryParam("limit", "10").
		AndQueryParam("offset", "0").
		WithResult(&users).
		Send(ctx)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Printf("Users: %+v\n", users)

	// Using path parameters
	var project User
	_, _, err = request.NewHTTPRequest(c).
		WithGet("/users/{userId}/projects/{projectId}").
		AndPathParam("userId", "123").
		AndPathParam("projectId", "456").
		WithResult(&project).
		Send(ctx)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Printf("Project: %+v\n", project)

	// Using callbacks
	_, _, err = request.NewHTTPRequest(c).
		WithGet("/users/123").
		WithResult(&user).
		WithOnSuccess(func(ctx context.Context, response request.HTTPResponse) error {
			fmt.Println("Request succeeded with status:", response.StatusCode())
			return nil
		}).
		WithOnError(func(ctx context.Context, response request.HTTPResponse, err error) error {
			fmt.Println("Request failed with status:", response.StatusCode())
			return err
		}).
		Send(ctx)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	// Using form body
	var loginResponse struct {
		Token string `json:"token"`
	}
	_, _, err = request.NewHTTPRequest(c).
		WithPost("/login").
		WithFormBody(map[string]string{
			"username": "user@example.com",
			"password": "password123",
		}).
		WithResult(&loginResponse).
		Send(ctx)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	fmt.Printf("Login token: %s\n", loginResponse.Token)
}
```

## Development

Clone the repository and run dev container:
```sh
docker-compose run --rm -u "$UID:$GID" --service-ports dev bash
```

Run lint and tests in container:
```sh
task lint
task tests
```

Run HTTP server with documentation:
```sh
task godoc
```

Open `http://localhost:6060/pkg/github.com/keboola/go-client/pkg/` in browser.

## License

MIT licensed, see [LICENSE](./LICENSE) file.
