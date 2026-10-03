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
  INSERT INTO outbox (event_id, topic, payload, status) VALUES (..., 'PENDING')
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

The relay is usually a poller. CDC (e.g. Debezium tailing the binlog) is the
alternative when polling load or latency becomes a problem.

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
- **`enable.idempotence=true` does not cover this.** Kafka's idempotent
  producer only dedupes retries within one producer session. A restarted
  relay gets a new producer ID, and the broker sees a new message.

This is the same rule as [API idempotency](./api-idempotency.md): the
record of intent and the business change have to commit together, or a
crash between them leaves you guessing.

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
