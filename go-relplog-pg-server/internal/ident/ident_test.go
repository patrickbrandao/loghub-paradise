package ident

import "testing"

func TestValidateSpace(t *testing.T) {
	for _, ok := range []string{"default", "com.acme.store", "a", "service.tenant.91282", "a-b"} {
		if err := ValidateSpace(ok); err != nil {
			t.Errorf("%q refused: %v", ok, err)
		}
	}
	long := ""
	for i := 0; i < 65; i++ {
		long += "a"
	}
	for _, bad := range []string{"", ".", "..", "a..b", ".x", "x.", "UPPER", "under_score", "spa ce", long, "_ctl"} {
		if err := ValidateSpace(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestValidFrom(t *testing.T) {
	if !ValidFrom("01962a3f-07bc-70b4-ae05-c57e7541d8a8") {
		t.Error("v7 refused")
	}
	for _, bad := range []string{"01962A3F-07BC-70B4-AE05-C57E7541D8A8", "00000000-0000-0000-0000-000000000000",
		"01962a3f-07bc-90b4-ae05-c57e7541d8a8", "01962a3f-07bc-70b4-ce05-c57e7541d8a8", "01962a3f07bc70b4ae05c57e7541d8a8"} {
		if ValidFrom(bad) {
			t.Errorf("%q accepted", bad)
		}
	}
}
