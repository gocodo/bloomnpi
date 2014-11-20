package npi

import (
	"github.com/go-contrib/uuid"
	"strings"
)

func makeKey(values ...string) string {
	key := "[" + strings.Join(values, "][") + "]"
	return uuid.NewV3(uuid.NamespaceOID, key).String()
}