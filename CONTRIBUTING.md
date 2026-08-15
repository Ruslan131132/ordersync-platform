# Contributing

This is a portfolio / learning project. Small, focused PRs that deepen the sync path are preferred over new surface area.

## Local checks

```bash
go test ./... -count=1
golangci-lint run ./...
```

## Interview talking points (short)

1. **Rate limits** — why per-method keys matter; local token bucket vs shared Redis limiter.
2. **Adaptive batching** — batch size follows fresh volume, not a fixed poll.
3. **Locks** — Redis TTL lock for workers + `SKIP LOCKED` for rows.
4. **Outbox** — DB is source of truth; Kafka would replace the in-process publisher only.
5. **Idempotency** — ledger keyed by content hash; HTTP `Idempotency-Key` for triggers.

See README for architecture and design decisions.
