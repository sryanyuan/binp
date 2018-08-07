package utils

import "fmt"

// GetTableKey returns the key of table
func GetTableKey(schema, table string) string {
	return fmt.Sprintf("%s.%s", schema, table)
}
