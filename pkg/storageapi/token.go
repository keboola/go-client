package storageapi

import (
	"github.com/keboola/go-client/pkg/client"
)

// Token https://keboola.docs.apiary.io/#reference/tokens-and-permissions/token-verification/token-verification
type Token struct {
	ID       string     `json:"id"`
	Token    string     `json:"token"`
	IsMaster bool       `json:"isMasterToken"`
	Owner    TokenOwner `json:"owner"`
}

// TokenOwner - owner of Token.
type TokenOwner struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
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
		AndHeader("X-StorageApi-Token", token)
	return client.NewAPIRequest(result, request)
}
