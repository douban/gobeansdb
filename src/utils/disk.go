package utils

import (
	"loghub"
	"os"
)

func Remove(path string) error {
	loghub.Default.Logf(loghub.INFO, "remove path: %s", path)
	return os.Remove(path)
}
