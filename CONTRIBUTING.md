# Contributing

This is a portfolio / learning project. Small PRs that deepen the sync path (correctness, failure handling, observability) are preferred over new surface area.

## Before opening a PR

```bash
make lint
make test
make test-integration   # Docker required
```

- Keep `api` and `worker` as the only long-running processes.
- New SQL goes into a new numbered file in `migrations/`; never edit an applied migration.
- Anything that changes the claim / complete / outbox path needs an integration test in `internal/integration`.
- No real marketplace endpoints, tokens or customer data, even in tests.

## Interview talking points

Questions this codebase is built to answer, with the place to look:

1. **"Two workers grabbed the same order — how do you prevent it?"** `FOR UPDATE SKIP LOCKED` for the claim, then a lease token for the rest of the work, because the HTTP call can't run inside the transaction. See `ClaimOrders` / `CompleteOrder` in `internal/store/order.go`.
2. **"The worker paused for longer than its lease. What happens?"** The status update is fenced by `lease_token`, so the stale worker gets `ErrLeaseLost`. Its ledger row still lands, so the new owner skips the remote call. This is covered by `TestExpiredLeaseIsReclaimedAndStaleCompletionIsFenced`.
3. **"The marketplace applied the write and then returned 503."** The `Idempotency-Key` is derived from order, method and content hash, so the retry is a replay. The mock simulates exactly this case, and `loadgen` fails if it ever records a duplicate.
4. **"Why Redis at all if Postgres does the locking?"** For leader election of periodic duties and for a rate-limit budget shared across replicas. Neither is a correctness dependency: the duties are idempotent, and the server enforces its own limit.
5. **"How would you add Kafka?"** Implement `outbox.Publisher`. The transactional outbox and the relay stay as they are, and delivery stays at-least-once with consumer dedupe by event id.
6. **"How do you deploy without losing in-flight work?"** Stop claiming, give in-flight orders a grace period, release whatever is left, then step down as leader. See `Worker.Run` in `internal/syncer/worker.go`.
7. **"How did you know it scales?"** The README benchmarks show that two replicas don't help when the job is limit-bound (shared budget) and nearly double throughput when it's concurrency-bound.

## Commit style

Conventional prefixes (`feat:`, `fix:`, `test:`, `docs:`, `chore:`, `refactor:`), imperative mood, one logical change per commit.
