package store

import (
	"strings"
	"testing"
)

func TestIsValidKeyString(t *testing.T) {
	testKey := []struct {
		x      string
		expect bool
	}{
		{"hello", true},
		{"?helo", false},
		{" hello", false},
		{"@hello", false},
		{"你好", true},
		{"hello*", true},
		{strings.Repeat("a", MAX_KEY_LEN-1), true},
		{strings.Repeat("a", MAX_KEY_LEN), true},
		{strings.Repeat("a", MAX_KEY_LEN+1), false},
		{"hello\n", false},
		{"he llo", false},
		{"he\tllo", false},
		{"he\rllo", false},
	}

	for _, tt := range testKey {
		result := IsValidKeyString(tt.x)
		if result != tt.expect {
			if tt.expect {
				t.Fatalf("key %s should be %s", tt.x, "valid")
			} else {
				t.Fatalf("key %s should be %s", tt.x, "invalid")
			}
		}
	}

}
