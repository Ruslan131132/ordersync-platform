package sync

import "testing"

func TestAdaptiveBatchSize(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		volume int
		want   int
	}{
		{"empty", 0, 5},
		{"small", 10, 5 + (10*(40-5))/40},
		{"at_soft", 40, 5 + (40*(40-5))/40},
		{"above_soft", 100, 40 + ((100-40)*(100-40))/(160-40)},
		{"huge", 10000, 100},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := AdaptiveBatchSize(tc.volume, 5, 40, 100)
			if got != tc.want {
				t.Fatalf("volume=%d got=%d want=%d", tc.volume, got, tc.want)
			}
			if got < 5 || got > 100 {
				t.Fatalf("out of bounds: %d", got)
			}
		})
	}
}
