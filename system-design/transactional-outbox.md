# Transactional Outbox

A service writes to its database and publishes an event to a broker. Those
are two systems, and there is no transaction that spans both. Every
"save, then send" path has a gap between the two writes, and something
eventually lands in it.

The outbox doesn't close the gap. It moves the event into the same database
transaction as the state change, so the two can no longer disagree, and
leaves a separate process to deliver it later. This note covers the
**producer side**: getting the event out reliably. Handling the duplicates
that follow is the consumer's job; see [event idempotency](./event-idempotency.md).

---

## The Dual-Write Problem

```java
@Transactional
public void placeOrder(...) {
    orderRepository.save(order);
    kafkaTemplate.send("order-created", event);
}
```

This breaks in three ways, and only two of them involve a failure.

| Scenario | DB | Broker | What happens |
| --- | --- | --- | --- |
| **Ghost event** — send succeeds, commit fails | rolled back | has event | Payment, notification, and settlement act on an order that doesn't exist. A sent message can't be recalled. |
| **Lost event** — async send fails unnoticed, commit proceeds | has order | nothing | Order sits in "paid" forever. No error, no alert. |
| **Visibility race** — both succeed | not yet committed | has event | Send happens before commit. The consumer calls `GET /orders/{id}` and gets a 404. |

Two traps make this worse than it looks:

- `kafkaTemplate.send()` is asynchronous. It returns a `CompletableFuture`
  immediately, whatever `acks` is set to. If nobody waits on the future, a
  failed send goes unnoticed.
- `acks` doesn't solve this. The problem is the time gap between two writes,
  not how durable either one is.

---

## Step One: Send After Commit

Move the send after the commit. In Spring this is
`@TransactionalEventListener(phase = AFTER_COMMIT)`, and it's the most common
fix in practice.

It doesn't make the two writes atomic. It **controls which way they fail**.

| | Send before commit | Send after commit |
| --- | --- | --- |
| Ghost event | yes | **no** |
| Visibility race | yes | **no** |
| Lost event | yes | **still yes** |

Every remaining failure is now "the DB has it, the event is missing," and
that's the side you can repair. **A late notification beats a notification
for an order that doesn't exist. You can resend a late one; you can't take
back the other.**

The catch is that "repairable" only holds if you know what to resend. After
a crash, the order row exists, but nothing records that an event was owed.

---

## Step Two: The Outbox

Write the event into an `outbox` table **in the same transaction** as the
order. Both commit or neither does. The problem changes from two systems
to two tables in one database, and a single database can already handle
that.

```
BEGIN
  INSERT INTO orders ...
  INSERT INTO outbox (event_id, aggregate_id, topic, payload, status)
    VALUES (..., 'order-7', ..., 'PENDING')
COMMIT
```

The name means what it says: it's the **outbound mail tray**, a list of what
still needs to go out, not a log of what has been sent. At commit time
nothing has been published.

A separate **relay** drains it:

1. Read `PENDING` rows.
2. Send to the broker and **wait for the ack**, either with `.get()` or by
   acting only in the success callback.
3. Mark the row `SENT`.

The relay is usually a scheduled job inside the service itself, not a
separate system. Running it is its own topic; see
[Running the Relay](#running-the-relay).

---

## What It Guarantees: At-Least-Once

If the relay dies between step 2 and step 3, it restarts, sees the row still
`PENDING`, and sends it again. Losses become duplicates. The trade is the
same one as before: a duplicate can be handled, a lost event can't.

So the outbox gives **at-least-once delivery**. Exactly-once *delivery*
isn't on offer. What you can get is an exactly-once *effect* from
at-least-once delivery plus an idempotent receiver (see
[exactly-once](./exactly-once.md)).

The producer's part of that deal:

- **Assign `event_id` at outbox insert time** and carry it in the message
  header. Every resend of that row carries the same ID, so the consumer can
  dedupe on it. An ID generated at send time is useless.
- **`enable.idempotence=true` is necessary but not sufficient.** It dedupes
  network retries within one producer session. A restarted relay gets a new
  producer ID, so resends after a restart look new to the broker. That gap
  is what `event_id` covers.

The producer can't close that gap itself. A relay that died before marking
`SENT` can't tell whether the broker got the message, so it has to resend.
Only the receiver can make the final call on duplicates.

This is the same rule as [API idempotency](./api-idempotency.md): the
record of intent and the business change have to commit together, or a
crash between them leaves you guessing.

---

## Running the Relay

### Ordering: where it actually breaks

Order matters only **within one aggregate** (one order's `created` →
`cancelled`), never across orders. Two different mechanisms protect two
different legs:

| Leg | Who keeps order | How |
| --- | --- | --- |
| outbox → broker | **the relay** | calls `send()` in outbox-id order, per aggregate |
| broker → consumer | Kafka | key = `aggregate_id` → same partition; order holds only within a partition |

Kafka keeps **arrival order**, not business order. A key doesn't help if
the relay sent things in the wrong order to begin with.

`enable.idempotence` is what keeps the first leg honest under retries: it
numbers sends **per (producer, partition)** and rejects a gap, so one
producer's `send()` order survives the client's internal retries. It knows
nothing across producers. Two producers writing one partition interleave in
whatever order they arrive.

So with key and idempotence both set, order still breaks when the relay
itself calls `send()` out of order:

- **The relay skips a failed row.** id 10 (`created`) fails, the relay
  leaves it `PENDING`, sends id 11 (`cancelled`), and picks up 10 on the
  next poll. That resend is a new `send()` call, after 11. The client's
  retries keep order; the application's retries don't. This happens with a
  single relay.
- **Two relays run at once.** Each service pod runs the scheduler, or a
  rolling deploy briefly overlaps old and new. Two producers mean no order
  between them. `SKIP LOCKED` stops them grabbing the same row, but that's
  for efficiency, since consumers dedupe anyway. It does nothing for order;
  it hands neighboring rows to different relays in parallel.

### One relay is the default

"Three relays" is usually three service pods that each happen to run the
scheduler, not a throughput decision. Run **one active relay**: a single
deployment, or a scheduler lock like ShedLock so only one pod runs it and
the rest are standbys. That fixes the two-relay case. Batching (send a page
asynchronously, wait for all acks, update in bulk) gets a single relay far
past one-row-at-a-time. Measure before concluding one isn't enough.

When it really isn't, the next step is usually **CDC** (Debezium reading
the binlog/WAL), not hand-built sharding. The log is already one stream in
commit order, so a single reader keeps order with no polling queries.
Progress becomes one log position instead of per-row status, so it's still
at-least-once. The cost moves to operations: if the reader stalls, MySQL
may purge the binlog it needs, and Postgres keeps WAL until the disk fills.
Splitting polling relays across fixed shards with leases is the last resort.

### Failures: block the aggregate, not the relay

Fix for the skipped-row case: when a row fails, **later rows of the same
aggregate wait.** Other aggregates keep flowing. Blocking the whole batch
on one failure stalls everything. The outbox needs an `aggregate_id` column
for this.

Then the row that never succeeds (payload over the broker's size limit,
say). It blocks that order forever, and **nobody notices**: the broker never
received it, so it has nothing to report. That's the silent "lost event"
again, in a new shape. So:

- Cap retries, then park the row in place as `FAILED` with the error.
  A Kafka DLQ topic doesn't help when Kafka is the thing rejecting it.
- **Keep the aggregate blocked.** Releasing id 11 without id 10 delivers a
  cancel for an order that was never created.
- Leave id 11 `PENDING`; don't mark it too. "Blocked" is derived from
  "an earlier row of this aggregate is `FAILED`". Store the fact once, and
  fixing id 10 unblocks the rest without touching them.
- Alert on `FAILED` count and on the **age of the oldest `PENDING` row**.
  The outbox only turns a silent loss into a visible delay if someone is
  watching the delay.

---

## When to Use What

Don't decide by how important the event sounds. Ask: **if this event
disappears, who notices, and when?**

| Event | Lost event is... | Use |
| --- | --- | --- |
| Analytics / marketing ("first purchase") | Recoverable: the source rows are in the DB, so a batch can backfill | `AFTER_COMMIT` |
| Order → shipping | Silent: the event is the only trigger for the next state, and the order stalls with no error | Outbox |

If the event is a **copy** of state that lives elsewhere, `AFTER_COMMIT` plus
an occasional reconciliation is enough. If the event is the **only trigger**
for the next step, use the outbox.

Two calibrations:

- `AFTER_COMMIT` losses aren't only the rare crash in a tiny window. **A
  five-minute broker outage drops five minutes of events at once.** Plan for
  that case.
- **The outbox handles publishing, not the whole workflow's consistency.** A
  transfer between accounts in one DB needs no event, just one transaction.
  Across services, the outbox only makes sure "debited" gets announced. If
  the credit fails, undoing the debit is a [saga](./saga.md) (planned) with
  compensation. "We use an outbox, so it's consistent" is the claim to
  push back on.

---

## Related

- [reliable messaging](./reliable-messaging.md): the consumer-side version
  of the same idea, with an in-flight list so a crashed worker's message
  isn't lost.
- [defining the problem](./defining-the-problem.md): "cost of a duplicate
  vs cost of a drop" is the question that picks the guarantee.
- [consistency models](./consistency-models.md): at-least-once plus
  idempotency is buying back correctness on top of a weak guarantee.
