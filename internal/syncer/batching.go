package syncer

// AdaptiveBatchSize maps the volume of claimable orders to a batch size.
// Low volume gives small batches (low latency, short leases); volume above the
// soft limit ramps linearly to maxSize at 4x the soft limit, amortising
// per-batch overhead when there is a backlog to drain.
func AdaptiveBatchSize(volume, minSize, soft, maxSize int) int {
	if volume <= 0 {
		return minSize
	}
	if volume <= soft {
		return clamp(minSize+volume*(soft-minSize)/max(soft, 1), minSize, maxSize)
	}
	upper := soft * 4
	if volume >= upper {
		return maxSize
	}
	return clamp(soft+(volume-soft)*(maxSize-soft)/max(upper-soft, 1), minSize, maxSize)
}

func clamp(v, lo, hi int) int {
	return min(max(v, lo), hi)
}
