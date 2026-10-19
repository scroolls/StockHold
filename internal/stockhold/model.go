package stockhold

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	ErrInvalid      = errors.New("invalid input")
	ErrNotFound     = errors.New("not found")
	ErrExists       = errors.New("already exists")
	ErrInsufficient = errors.New("insufficient stock")
	ErrConflict     = errors.New("conflict")
	ErrExpired      = errors.New("reservation expired")
)

var skuPattern = regexp.MustCompile(`^[A-Z0-9][A-Z0-9_-]{1,63}$`)
var idPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

const MaxQuantity int64 = 1_000_000_000

type Item struct {
	SKU       string    `json:"sku"`
	Name      string    `json:"name"`
	OnHand    int64     `json:"on_hand"`
	Available int64     `json:"available"`
	CreatedAt time.Time `json:"created_at"`
}

type Reservation struct {
	ID        string    `json:"id"`
	SKU       string    `json:"sku"`
	Quantity  int64     `json:"quantity"`
	Status    string    `json:"status"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func NormalizeSKU(s string) (string, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	if !skuPattern.MatchString(s) {
		return "", fmt.Errorf("%w: sku must be 2-64 ASCII letters, digits, _ or -", ErrInvalid)
	}
	return s, nil
}

func ValidateQuantity(n int64) error {
	if n < 1 || n > MaxQuantity {
		return fmt.Errorf("%w: quantity must be between 1 and %d", ErrInvalid, MaxQuantity)
	}
	return nil
}

func ValidateKey(key string) error {
	if len(key) < 8 || len(key) > 128 || strings.TrimSpace(key) != key {
		return fmt.Errorf("%w: Idempotency-Key must be 8-128 characters without surrounding spaces", ErrInvalid)
	}
	for _, r := range key {
		if r < 33 || r > 126 {
			return fmt.Errorf("%w: Idempotency-Key must contain printable ASCII only", ErrInvalid)
		}
	}
	return nil
}

func ValidateID(id string) error {
	if !idPattern.MatchString(id) {
		return fmt.Errorf("%w: invalid reservation ID", ErrInvalid)
	}
	return nil
}

func requestHash(parts ...any) []byte {
	data, _ := json.Marshal(parts)
	h := sha256.Sum256(data)
	return h[:]
}

func newID(random [16]byte) string {
	random[6] = (random[6] & 0x0f) | 0x40
	random[8] = (random[8] & 0x3f) | 0x80
	s := hex.EncodeToString(random[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
