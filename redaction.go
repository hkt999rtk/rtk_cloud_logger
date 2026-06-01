package cloudlogger

import "strings"

const RedactedValue = "[REDACTED]"

func RedactEvent(event LogEvent) LogEvent {
	if len(event.Fields) == 0 {
		return event
	}
	event.Fields = redactMap(event.Fields)
	return event
}

func redactMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		if sensitiveFieldName(key) {
			out[key] = RedactedValue
			continue
		}
		if nested, ok := value.(map[string]any); ok {
			out[key] = redactMap(nested)
			continue
		}
		out[key] = value
	}
	return out
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
