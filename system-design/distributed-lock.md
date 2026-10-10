# Distributed Lock

"Only one instance runs this at a time" across several processes. The usual
tools (ShedLock, Redis `SET NX PX`) all look different, but under the hood
they're the same thing: a **lease**, a lock that expires after a set time.
The lease expires whether or not the holder has finished. Most of what
matters about distributed locks follows from that one fact.

---

## What a Lock Actually Is

### ShedLock (JDBC)

A library that keeps a scheduled job (usually Spring `@Scheduled` +
`@SchedulerLock(name = "outboxRelay")`) running on one pod at a time. With
the JDBC provider the lock is **one row per job name**:

```
name (PK)     | lock_until | locked_at | locked_by
--------------+------------+-----------+----------
outboxRelay   | 10:00:30   | 10:00:00  | pod-A
cleanupJob    | 10:00:05   | 09:59:35  | pod-B
```

The unit is the job name, not the pod. `locked_by` only records who holds
it; it isn't part of the acquire condition.

Acquiring is a **conditional UPDATE**:

```sql
UPDATE shedlock
SET lock_until = now() + INTERVAL 30 SECOND, locked_at = now(), locked_by = 'pod-A'
WHERE name = 'outboxRelay' AND lock_until <= now();
```

One row changed means acquired; zero means someone else holds it. (The
first run of a job is an INSERT; a PK conflict means failed.) Releasing is
also an UPDATE that pulls `lock_until` back.

The DB's row lock lasts only until that UPDATE commits (one statement
under autocommit). It makes "is it free? then it's mine" atomic between
pods that use the same conditional UPDATE. The 30-second lease that follows
is a **logical lock**: a timestamp whose meaning lives in that
`WHERE lock_until <= now()` clause. The database doesn't know what it
means, and nothing ties it to the outbox rows it's meant to protect. An
UPDATE without that clause overwrites the lease, and code that skips the
lock touches the outbox freely. Nothing checks whether pod-A is still
alive, either.

- `lockAtMostFor` is the lease length. It's what lets another pod take over
  if the holder dies without releasing. Too long and recovery after a crash
  is slow. Too short and it expires under a holder that's still working.
- `lockAtLeastFor` keeps the lock held for a minimum time even if the job
  finishes instantly, so a pod whose clock is slightly behind doesn't run
  the same tick again.

### Redis `SET NX PX`

```
SET lock:outboxRelay pod-A NX PX 30000
```

Same thing in a different store. Acquire is atomic, expiry is a TTL, and
nobody checks whether the holder is alive.

| | ShedLock (JDBC) | Redis `SET NX PX` |
| --- | --- | --- |
| Acquire | conditional UPDATE, atomic | `SET NX`, atomic |
| Expiry | `lock_until` timestamp | TTL |
| Checks holder liveness | no | no |
| Old-holder problem | yes | yes |
| Fencing token | not built in | not built in |

Atomic acquisition was never the hard part. Both get it right. The hard
part comes **after** acquiring.

Redis adds one failure of its own: replication is asynchronous by default.
If the primary dies right after accepting the lock and a replica that never
saw it is promoted, two holders exist without any pause at all. Redlock is
the attempt to fix that; whether it does is disputed (see
[cache stampede](./cache-stampede.md#distributed-lock-caveats)).

---

## The Old-Holder Problem

With `lockAtMostFor = 30s`, in the outbox relay:

1. A acquires the lock and reads `PENDING` event 10 (`created`).
2. A stalls for 40 seconds: GC pause, slow broker, CPU starvation.
   The lease expires.
3. B acquires, sends 10 (`created`) and 11 (`cancelled`), marks them `SENT`.
4. A wakes up, **still believing it holds the lock**, and sends 10 again.

The consumer sees `created → cancelled → created`: a duplicate and a
reordering. The lock didn't stop A. A lease expiring doesn't stop the
holder's code; it only lets someone else start.

This is different from the crash-between-send-and-mark duplicate in the
[outbox](./transactional-outbox.md#what-it-guarantees-at-least-once). That
one happens with one relay. This one is **two holders at once**.

### Why the holder can't check for itself

Check the lock right before sending?

```
A: read lock row → "still mine"     (t = 29.9s)
A: ── paused 20s ──                 ← gap between check and act
   (lease expires, B acquires, B sends)
A: send(10)                         ← acting on a stale check
```

Any check made by the holder is a separate step from the write it guards,
and the holder can pause between them. Checking more often only makes the
gap smaller. It never closes it. This is the same **check-then-act** gap
that breaks every pattern below unless the check and the write are one
atomic operation.

---

## Fencing Tokens

Every successful acquire returns a **strictly increasing number**. Every
write carries it. **The resource receiving the write** rejects any token
smaller than the largest it has seen.

```
A acquires → token 33
A pauses, lease expires
B acquires → token 34
B writes with 34 → resource remembers 34   ✅
A writes with 33 → 33 < 34, rejected       ❌
```

The check and the write happen together inside the resource, so there's no
gap. A doesn't need to know it lost the lock; the rejection tells it.

It's the same compare-and-set as optimistic locking, attached to a
different thing:

| | Optimistic lock `version` | Fencing token |
| --- | --- | --- |
| Numbered | each data row | each lock grant |
| Rejects | "someone changed this row since you read it" | "someone acquired the lock after you" |
| Checked by | `WHERE version = ?` in the DB | whatever receives the write |

### Issuing the token

Add a `token` column and bump it **inside the acquire UPDATE**:

```sql
UPDATE shedlock
SET lock_until = now() + INTERVAL 30 SECOND, locked_by = 'pod-A',
    token = token + 1
WHERE name = 'outboxRelay' AND lock_until <= now();
```

Only the winner changes the row, so only the winner gets a number, and no
number is issued twice. The atomicity the acquire already had is all it
needs.

Ways to get it wrong:

- **`token = now()`.** Time isn't strictly increasing. Clocks step
  backward under NTP correction or failover, and two acquires in the same
  second get the same value. Fencing depends on strictly increasing.
- **`SELECT token`, then write `token + 1` from the app.** B reads 33
  before A acquires, A sets 34, A's lease expires, B sets 34. Two holders,
  one token. Let the DB compute `token + 1` from the current value.

### Reading the token back

MySQL's UPDATE returns a row count, not values, so the holder needs a way
to learn its token. The question is whether it reads **the value it
wrote** or **whatever is in the table now**.

- **Postgres: `RETURNING`.** One statement; the value comes back with the
  update.
  ```sql
  UPDATE shedlock SET ..., token = token + 1
  WHERE name = 'outboxRelay' AND lock_until <= now()
  RETURNING token;
  ```
- **MySQL: `LAST_INSERT_ID(expr)`.** Stores the value in a per-connection
  variable as it writes it. The follow-up `SELECT LAST_INSERT_ID()` reads
  that variable, not the table, so another session can't change it.
  ```sql
  UPDATE shedlock SET ..., token = LAST_INSERT_ID(token + 1)
  WHERE name = 'outboxRelay' AND lock_until <= now();
  SELECT LAST_INSERT_ID();
  ```
- **Same transaction.** `BEGIN; UPDATE ...; SELECT token ...; COMMIT;`
  The UPDATE's row lock holds until commit, so no one can bump the token
  in between. What makes it safe is the lock, not snapshot visibility.

In every case, trust the token only if the UPDATE changed one row. On zero
rows there's no lock, and what you read is someone else's token (or a stale
`LAST_INSERT_ID`).

What breaks is a **separate autocommit SELECT** on the table. The UPDATE
commits and its row lock is released. If A pauses before the SELECT, B can
acquire and bump to 35, and A reads B's token as its own. Fencing is then
silently off.

### Fencing only works where the write lands

The token protects only resources that check it. The outbox relay makes two
writes:

| Write | Can it check a token? | How |
| --- | --- | --- |
| `UPDATE outbox SET status = 'SENT'` | yes | join the lock row and require `token = :mine`; zero rows means A has been replaced and stops |
| Kafka `send()` | no | the broker doesn't know your token |

`enable.idempotence` doesn't fill the Kafka side. It dedupes retries
**per (producer, partition)**. A and B are different producers, so their
copies of event 10 are two different messages to the broker, and it
doesn't reorder anything either.

The DB-side fence also runs **after** the send, so it keeps the status
column honest but can't recall a message. For what reaches Kafka, the
check moves to where the effect finally lands, the consumer:

- dedupe on `event_id` (duplicates)
- remember the last `seq` per aggregate and ignore anything older
  (reordering)

That second rule *is* a fencing check, with the consumer as the resource.

Kafka does have real fencing for transactional producers: two producers
sharing one `transactional.id` bump an epoch on `initTransactions()`, and
the older one gets `ProducerFencedException`. It doesn't extend to the DB
update, so it doesn't settle the outbox case on its own.

---

## When to Use What

Ask: **if two holders run at once, what breaks?**

| Two holders means... | Enough | Example |
| --- | --- | --- |
| Wasted work only | a lease (ShedLock, `SET NX PX`), TTL above p99 | [cache stampede](./cache-stampede.md) refresh |
| Duplicates or reordering the receiver already absorbs | a lease, plus the idempotent receiver you need anyway | outbox relay with `event_id` / `seq` checks |
| Corrupted state, or a side effect that must happen once | a check **at the resource**: fencing token, idempotency key, conditional update. The lock alone can't do it | payment: PG idempotency key on the order ID, `UPDATE orders ... WHERE status = 'PENDING'` |

A lease with no fence is an efficiency tool: it makes overlap rare, not
impossible. If correctness depends on "never two at once," the lock isn't
what guarantees it. The resource's check is.

This holds with a single store too. Lock `payment:order-7` for 30
seconds, call the PG, and have the PG take 40: the lease expires, the next
holder sees the order still unpaid (nothing has been written yet), and
charges again. The lock worked as designed. The question is never "how many
stores are involved" but **"can the lock be lost without the holder
knowing?"** With a lease, it always can.

---

## Related

- [transactional outbox](./transactional-outbox.md): where the relay and
  its one-active-instance rule come from.
- [cache stampede](./cache-stampede.md): a case where a lease without
  fencing is the right amount.
- [inventory concurrency control](./inventory-concurrency-control.md):
  locking inside one database, where the row lock lasts as long as the
  transaction.
