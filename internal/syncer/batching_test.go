package syncer

import "testing"

func TestAdaptiveBatchSize(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		volume int
		want   int
	}{
		{"empty", 0, 5},
		{"tiny", 1, 5},
		{"small", 10, 5 + 10*35/40},
		{"at soft limit", 40, 40},
		{"above soft limit", 100, 40 + 60*60/120},
		{"at 4x soft", 160, 100},
		{"huge backlog", 1_000_000, 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := AdaptiveBatchSize(tc.volume, 5, 40, 100); got != tc.want {
				t.Fatalf("volume=%d got=%d want=%d", tc.volume, got, tc.want)
			}
		})
	}
}

func TestAdaptiveBatchSizeIsMonotonicAndBounded(t *testing.T) {
	t.Parallel()
	prev := 0
	for v := 0; v <= 1000; v++ {
		got := AdaptiveBatchSize(v, 5, 40, 100)
		if got < 5 || got > 100 {
			t.Fatalf("volume=%d out of bounds: %d", v, got)
		}
		if got < prev {
			t.Fatalf("volume=%d: batch shrank from %d to %d", v, prev, got)
		}
		prev = got
	}
}

func BenchmarkAdaptiveBatchSize(b *testing.B) {
	for i := range b.N {
		AdaptiveBatchSize(i%2000, 5, 40, 200)
	}
}
