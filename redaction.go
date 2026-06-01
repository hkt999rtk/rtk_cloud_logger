package cloudlogger

import (
	"net/url"
	"strings"
)

var sensitiveBodyKeys = map[string]struct{}{
	"authorization":      {},
	"auth_header":        {},
	"bearer_token":       {},
	"token":              {},
	"access_token":       {},
	"refresh_token":      {},
	"cookie":             {},
	"cookies":            {},
	"password":           {},
	"client_secret":      {},
	"database_dsn":       {},
	"dsn":                {},
	"turn_shared_secret": {},
	"linode_token":       {},
	"godaddy_token":      {},
	"s3_secret":          {},
	"smtp_password":      {},
	"cloudwatch_secret":  {},
	"private_key":        {},
	"certificate_key":    {},
}

func RedactEvent(event LogEvent) LogEvent {
	if event.Body != nil {
		event.Body = redactMap(event.Body)
	}
	return event
}

func redactMap(input map[string]any) map[string]any {
	output := make(map[string]any, len(input))
	for key, value := range input {
		if isSensitiveBodyKey(key) {
			output[key] = redactedMarker
			continue
		}
		output[key] = redactValue(value)
	}
	return output
}

func redactValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return redactMap(typed)
	case string:
		if looksLikeCredentialDSN(typed) || strings.Contains(typed, "-----BEGIN PRIVATE KEY-----") {
			return redactedMarker
		}
		return typed
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = redactValue(item)
		}
		return out
	default:
		return typed
	}
}

func isSensitiveBodyKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
	_, ok := sensitiveBodyKeys[normalized]
	return ok
}

func looksLikeCredentialDSN(value string) bool {
	parsed, err := url.Parse(value)
	if err != nil || parsed.User == nil {
		return false
	}
	_, hasPassword := parsed.User.Password()
	return hasPassword
}
