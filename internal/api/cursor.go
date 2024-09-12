package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"time"
)

func validateCursor(s string) error {
	if s == "" {
		return nil
	}
	b, e := base64.RawURLEncoding.DecodeString(s)
	if e != nil {
		return e
	}
	var c struct {
		At time.Time `json:"at"`
		ID string    `json:"id"`
	}
	if e = json.Unmarshal(b, &c); e != nil {
		return e
	}
	if c.At.IsZero() {
		return errors.New("missing timestamp")
	}
	_, e = uuid.Parse(c.ID)
	return e
}
