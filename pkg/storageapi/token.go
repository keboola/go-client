package storageapi

import (
	"context"
	"time"

	"github.com/relvacode/iso8601"

	"github.com/keboola/go-client/pkg/client"
)

// Token https://keboola.docs.apiary.io/#reference/tokens-and-permissions/token-verification/token-verification
type Token struct {
	Token                 string                        `json:"token"` // set manually from request
	ID                    string                        `json:"id"`
	Description           string                        `json:"description"`
	IsMaster              bool                          `json:"isMasterToken"`
	CanManageBuckets      bool                          `json:"canManageBuckets"`
	CanManageTokens       bool                          `json:"canManageTokens"`
	CanReadAllFileUploads bool                          `json:"canReadAllFileUploads"`
	CanPurgeTrash         bool                          `json:"canPurgeTrash"`
	Created               iso8601.Time                  `json:"created"`
	Refreshed             iso8601.Time                  `json:"refreshed"`
	Expires               iso8601.Time                  `json:"expires"`
	IsExpired             bool                          `json:"isExpired"`
	IsDisabled            bool                          `json:"isDisabled"`
	Owner                 TokenOwner                    `json:"owner"`
	Admin                 *TokenAdmin                   `json:"admin,omitempty"`
	Creator               *CreatorToken                 `json:"creatorToken,omitempty"`
	BucketPermissions     map[BucketID]BucketPermission `json:"bucketPermissions,omitempty"`
	ComponentAccess       []string                      `json:"componentAccess,omitempty"`
}

// TokenAdmin - admin part of the token that should exists if the token is a master token.
type TokenAdmin struct {
	Name                 string   `json:"name"`
	Id                   int      `json:"id"`
	IsOrganizationMember bool     `json:"isOrganizationMember"`
	Role                 string   `json:"role"`
	Features             Features `json:"features"`
}

// TokenOwner - owner of Token.
type TokenOwner struct {
	ID       int      `json:"id"`
	Name     string   `json:"name"`
	Features Features `json:"features"`
}

type CreatorToken struct {
	ID          int    `json:"id"`
	Description string `json:"description"`
}

// ProjectID returns ID of project to which the token belongs.
func (t *Token) ProjectID() int {
	return t.Owner.ID
}

// ProjectName returns name of project to which the token belongs.
func (t *Token) ProjectName() string {
	return t.Owner.Name
}

// VerifyTokenRequest https://keboola.docs.apiary.io/#reference/tokens-and-permissions/token-verification/token-verification
func VerifyTokenRequest(token string) client.APIRequest[*Token] {
	result := &Token{}
	request := newRequest().
		WithResult(result).
		WithGet("tokens/verify").
		AndHeader("X-StorageApi-Token", token).
		WithOnSuccess(func(_ context.Context, _ client.Sender, _ client.HTTPResponse) error {
			result.Token = token
			return nil
		})
	return client.NewAPIRequest(result, request)
}

type BucketPermission string

const (
	BucketPermissionRead  BucketPermission = "read"
	BucketPermissionWrite BucketPermission = "write"
)

type createTokenOptions struct {
	Description           string            `writeas:"description"`
	BucketPermissions     map[string]string `writeas:"bucketPermissions" writeoptional:"true"`
	ComponentAccess       []string          `writeas:"componentAccess" writeoptional:"true"`
	CanManageBuckets      bool              `writeas:"canManageBuckets"`
	CanReadAllFileUploads bool              `writeas:"canReadAllFileUploads"`
	CanPurgeTrash         bool              `writeas:"canPurgeTrash"`
	ExpiresIn             int               `writeas:"expiresIn" writeoptional:"true"`
}

type createTokenOption func(*createTokenOptions)

// WithDescription sets the token's description.
func WithDescription(description string) createTokenOption {
	return func(o *createTokenOptions) { o.Description = description }
}

// WithBucketPermission adds `bucket` to the set of buckets this token may read or write to, depending on the permission specified (`perm`).
func WithBucketPermission(bucketID BucketID, perm BucketPermission) createTokenOption {
	return func(o *createTokenOptions) {
		if o.BucketPermissions == nil {
			o.BucketPermissions = make(map[string]string)
		}
		o.BucketPermissions[bucketID.String()] = string(perm)
	}
}

// WithComponentAccess adds `component` to the list of components this token may access.
func WithComponentAccess(component string) createTokenOption {
	return func(o *createTokenOptions) { o.ComponentAccess = append(o.ComponentAccess, component) }
}

// WithCanManageBuckets gives the newly created token the ability to manage buckets.
func WithCanManageBuckets(canManageBuckets bool) createTokenOption {
	return func(o *createTokenOptions) { o.CanManageBuckets = canManageBuckets }
}

// WithCanReadAllFileUploads allows access to all file uploads. Without this permission, only files uplaoded using the new token are accessible.
func WithCanReadAllFileUploads(canReadAllFileUploads bool) createTokenOption {
	return func(o *createTokenOptions) { o.CanReadAllFileUploads = canReadAllFileUploads }
}

// WithCanPurgeTrash allows this token to permanently delete configurations.
func WithCanPurgeTrash(canPurgeTrash bool) createTokenOption {
	return func(o *createTokenOptions) { o.CanPurgeTrash = canPurgeTrash }
}

// WithExpiresIn sets the time until the token expires.
func WithExpiresIn(expiresIn time.Duration) createTokenOption {
	return func(o *createTokenOptions) { o.ExpiresIn = int(expiresIn.Seconds()) }
}

// CreateTokenRequest https://keboola.docs.apiary.io/#reference/tokens-and-permissions/tokens-collection/create-token
func CreateTokenRequest(opts ...createTokenOption) client.APIRequest[*Token] {
	options := &createTokenOptions{}
	for _, opt := range opts {
		opt(options)
	}

	result := &Token{}
	request := newRequest().
		WithResult(result).
		WithPost("tokens").
		WithFormBody(client.ToFormBody(client.StructToMap(options, nil)))
	return client.NewAPIRequest(result, request)
}

// RefreshTokenRequest https://keboola.docs.apiary.io/#reference/tokens-and-permissions/share-token/refresh-token
func RefreshTokenRequest(tokenID string) client.APIRequest[*Token] {
	result := &Token{}
	request := newRequest().
		WithResult(result).
		WithPost("tokens/{tokenId}/refresh").
		AndPathParam("tokenId", tokenID)
	return client.NewAPIRequest(result, request)
}
