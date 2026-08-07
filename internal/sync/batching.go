package sync

// AdaptiveBatchSize maps fresh order volume to a batch size.
// Idea: small queues → small batches (lower latency); large queues → larger
// batches (better API amortization) up to BatchMax, with a soft knee.
func AdaptiveBatchSize(freshVolume, min, soft, max int) int {
	if freshVolume <= 0 {
		return min
	}
	if freshVolume <= soft {
		// Linear grow from min toward soft as volume approaches soft.
		size := min + (freshVolume*(soft-min))/maxInt(soft, 1)
		return clamp(size, min, max)
	}
	// Above soft: ramp from soft to max by the time volume hits 4× soft.
	upper := soft * 4
	if freshVolume >= upper {
		return max
	}
	size := soft + ((freshVolume-soft)*(max-soft))/maxInt(upper-soft, 1)
	return clamp(size, min, max)
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
