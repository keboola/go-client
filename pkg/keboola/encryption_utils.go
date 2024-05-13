package keboola

import (
	"strings"

	"github.com/umisama/go-regexpcache"
)

// IsKeyToEncrypt returns true if given key represents an encrypted value.
func IsKeyToEncrypt(key string) bool {
	return strings.HasPrefix(key, "#")
}

// IsEncrypted returns true if value match format of encrypted value.
func IsEncrypted(value string) bool {
	currentFormatMatch := regexpcache.MustCompile(`^KBC::.+Secure.*::.+$`).MatchString(value)
	legacyFormatMatch := regexpcache.MustCompile(`^KBC::(Encrypted==|ComponentProjectEncrypted==|ComponentEncrypted==).+$`).MatchString(value)
	vaultFormatMatch := regexpcache.MustCompile(`\{\{\s*vault\.[^\}]+\}\}`).MatchString(value)
	return currentFormatMatch || legacyFormatMatch || vaultFormatMatch
}
