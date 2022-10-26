package storageapi

import (
	"context"

	"github.com/keboola/go-client/pkg/client"
)

// Token https://keboola.docs.apiary.io/#reference/tokens-and-permissions/token-verification/token-verification
type Token struct {
	Token    string      // set manually from request
	ID       string      `json:"id"`
	IsMaster bool        `json:"isMasterToken"`
	Owner    TokenOwner  `json:"owner"`
	Admin    *AdminToken `json:"admin,omitempty"`
}

// AdminToken - admin part of the token that should exists if the token is a master token.
type AdminToken struct {
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
