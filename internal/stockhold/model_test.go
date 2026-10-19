package stockhold

import "testing"

func TestNormalizeSKU(t *testing.T) {
	sku, err := NormalizeSKU("  cam-01 ")
	if err != nil || sku != "CAM-01" {
		t.Fatalf("got %q, %v", sku, err)
	}
	for _, input := range []string{"", "a", "a/b", "кириллица", "A B"} {
		if _, err := NormalizeSKU(input); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
}

func TestRequestHashSeparatesFields(t *testing.T) {
	a := requestHash("restock", "ABC1", int64(1))
	b := requestHash("restock", "ABC", int64(11))
	if string(a) == string(b) {
		t.Fatal("different requests have the same hash")
	}
}
