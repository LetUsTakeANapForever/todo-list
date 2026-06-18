package utils

import "time"

// ParseDueDate accepts multiple common input formats and returns a time.Time.
// Supported inputs (tried in order):
// - 2006-01-02
// - 20060102
// - 2006/01/02
// - time.RFC3339
// - time.RFC3339Nano
func ParseDueDate(value string) (time.Time, error) {
	layouts := []string{"2006-01-02", "20060102", "2006/01/02", time.RFC3339, time.RFC3339Nano}
	var lastErr error
	for _, l := range layouts {
		t, err := time.Parse(l, value)
		if err == nil {
			return t, nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}
