package keboola

import (
	"regexp"
	"strings"
)

// IsKeyToEncrypt returns true if given key represents an encrypted value.
func IsKeyToEncrypt(key string) bool {
	return strings.HasPrefix(key, "#")
}

// IsEncrypted returns true if value match format of encrypted value.
func IsEncrypted(value string) bool {
	currentFormatMatch := regexp.MustCompile(`^KBC::(ProjectSecure|ComponentSecure|ConfigSecure|ProjectWideSecure)(KV)?::.+$`).MatchString(value)
	legacyFormatMatch := regexp.MustCompile(`^KBC::(Encrypted==|ComponentProjectEncrypted==|ComponentEncrypted==).+$`).MatchString(value)
	return currentFormatMatch || legacyFormatMatch
}
