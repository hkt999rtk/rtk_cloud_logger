package cloudlogger

import "strings"

const RedactedValue = "[REDACTED]"

func RedactEvent(event LogEvent) LogEvent {
	event.Message = RedactText(event.Message)
	if len(event.Fields) == 0 {
		return event
	}
	event.Fields = redactMap(event.Fields)
	return event
}

func RedactText(value string) string {
	lower := strings.ToLower(value)
	for _, marker := range []string{
		"authorization",
		"bearer ",
		"password",
		"secret",
		"token",
		"payload",
		"private key",
		"-----begin",
	} {
		if strings.Contains(lower, marker) {
			return RedactedValue
		}
	}
	return value
}

func redactMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		if sensitiveFieldName(key) {
			out[key] = RedactedValue
			continue
		}
		out[key] = redactAny(value)
	}
	return out
}

func redactAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return redactMap(typed)
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = redactAny(item)
		}
		return out
	case string:
		return RedactText(typed)
	default:
		return value
	}
}

func sensitiveFieldName(name string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(name, "-", "_"))
	sensitive := []string{
		"authorization",
		"bearer",
		"token",
		"cookie",
		"password",
		"secret",
		"dsn",
		"credential",
		"payload",
		"private_key",
		"client_key",
		"access_key",
	}
	for _, item := range sensitive {
		if strings.Contains(normalized, item) {
			return true
		}
	}
	return false
}
