# Exactly-Once Effects

"Exactly-once delivery" can't be built. A sender that gets no answer can't
tell whether the message arrived, so it either sends again and risks a
duplicate or stays quiet and risks a loss. That's the Two Generals problem,
not a missing broker feature. What you can build is an **exactly-once
effect**: deliver at least once, and make the receiver turn repeats into
no-ops.

Inside one database that's easy. A transaction and a unique key do it.
This note is about the hard case: the effect happens **in someone else's
system** (a bank, a payment processor, an external ledger), and the call
that causes it can time out.

---

## A Timeout Is Not a Failure

A call across a boundary has three outcomes, not two:

| Outcome | What you know |
| --- | --- |
| Success response | it happened |
| Error response | it didn't happen (if the API says so) |
| **Timeout** | **nothing**. It may have happened, may still happen, or may never happen |

The third is called **in-doubt**. Treating it as either of the other two is
how duplicates and losses get in.

- **Keep an in-doubt state.** Record the attempt before calling out
  (`PROCESSING`), and leave it there on timeout. Don't mark it failed.
- **Don't tell the user "try again."** If the first attempt went through,
  a second click is a second charge. Tell them it's being processed.
- **Someone has to finish it.** Once the response is sent, the request
  thread is gone and the `PROCESSING` row is all that's left. A recovery
  job has to pick it up, find out what happened, and settle it. After a
  bounded number of tries, it goes to a person.

---

## Finding Out What Happened

### You need your own identifier on the call

To ask "did my request go through?" you need something to ask about. If
the receiver takes an idempotency key, that's it. If it doesn't, put **an
ID you chose** somewhere that comes back to you: a reference field, a memo
on a transfer, a description line. Amount and account aren't enough; the
same payee can legitimately get the same amount twice.

The ID has to come from the business identity (`merchant 6 + 2026-10-09`),
fixed before the first attempt. One generated per call is different on
every retry and identifies nothing.

### "Not found" can mean "not yet"

```
10:00:00  request sent
10:00:30  timeout. The receiver accepted it and queued it.
10:01:00  query → not found     ← it hasn't been processed yet
10:03:00  receiver processes it
```

Retrying at 10:01 sends the money twice. What you need from the receiver is
**finality**: a point after which "not found" means "never." It comes in a
few forms:

- **A time bound** in the contract: "not visible after N minutes means
  rejected."
- **Explicit status** instead of absence: `PENDING` / `FAILED` /
  `SUCCESS`. `FAILED` is final; `PENDING` means wait.
- **A daily close.** Once a business day is closed, that day's statement
  doesn't change. Even with no API-level promise, the next day's statement
  is final.

---

## Lines of Defense

From strongest to weakest. Use the first one available.

| Line | How | Needs |
| --- | --- | --- |
| **Prevent** | the receiver rejects a repeated key | receiver supports idempotency keys |
| **Narrow** | send your own ID, wait for finality, query, then retry; one retrier per item | a field to carry your ID, a finality rule |
| **Detect** | **reconciliation**: regularly match your records against the receiver's statement by ID | both records, a shared ID |
| **Escalate** | an error queue and a person | whatever the lines above couldn't settle |

Reconciliation finds three things: the same ID twice (duplicate), done on
your side but absent on theirs (loss), and present on theirs with no record
on yours (unknown origin). Run it on a schedule, since a duplicate can land
long after the retry. Nobody else will report it: a receiver that doesn't
check keys won't notice either.

---

## Retry or Wait?

Retrying trades a possible duplicate for less delay. Whether that's a good
trade depends on two costs:

> **If a duplicate costs more than the delay, don't retry. Wait until the
> outcome is final.**

The cost of a duplicate comes down to **who bears the harm, and whether
you can undo it alone**.

| | Paying a merchant (money out) | Debiting a customer (money in) |
| --- | --- | --- |
| Duplicate harms | you; a merchant you work with will usually return it or accept an offset | **the customer**: overdrafts, failed payments elsewhere, complaints, regulatory exposure. Refunding later doesn't undo it |
| Delay harms | the merchant's cash flow; a few hours is usually fine | your collection, which can wait |
| So | don't auto-retry; wait for finality or the next reconciliation, and settle quickly so the merchant isn't kept waiting | **never auto-retry**; wait for finality, however long |

Both wait. The reasons differ, and so does how strict the rule is.

### When a duplicate goes out anyway

Getting money back needs the other side's cooperation. In order of
preference:

1. **Offset**: deduct it from the next payout. Works with ongoing
   counterparties, and only if the contract already allows it.
2. **Ask for it back**, directly or through the bank's return process. Not
   guaranteed.
3. **Legal recovery**. Slow and expensive.

All three are decided before the incident: contract terms, the return
process, the reconciliation cadence. They can't be invented after it.

---

## Money Already Moved: Finish Forward

A common shape is two external steps: money moves (a debit or a charge),
then the result is recorded in another system (an external ledger or an
order system). When the first step succeeds and the second fails:

- **Backward recovery**: refund the money. The customer has to start over,
  and you give back money you'd already collected.
- **Forward recovery**: keep retrying the second step until it lands. The
  money side is done; what's left is systems agreeing with each other, with
  no customer involved.

Once the money has moved, finishing forward is almost always better for
both sides. Undoing is for when the second step can never succeed.

Finishing late raises a timing question. If the record lands tomorrow,
interest, due dates and balances may be computed as if the payment arrived
tomorrow. The record has to carry **when the money actually moved** (the
value date), not when the record was written (the booking date).

Check that the late-landing call actually has a value-date field, and that
it's filled with when the money moved, not when the call is made. A common
break is the retry path: the first attempt sends the right date, then the
recovery job rebuilds the request with `now()`, and the retry that finally
lands carries the wrong day.

---

## The Next Request While One Is In Doubt

What to do with a second request depends on what it is:

| Second request | Example | Treat it as |
| --- | --- | --- |
| **Same intent** | paying the same order again | a duplicate. The intent has an ID (order ID); reject on it or check its in-doubt state |
| **Independent intent** | sending the same amount to the same person again | possibly real. No ID marks it as a repeat, so the system can't decide. Ask the user ("you just sent this — again?") |
| **Independent, but depends on the first** | paying down a balance while an earlier payment is in doubt | real, but **can't be validated**: the balance depends on the first outcome. Block until the first is settled |

The third is easy to miss. It isn't a duplicate, but it still has to wait.

### Blocking it

The block lives on a row **both requests touch**: the narrowest row that
represents **what the second request depends on**. Here that's the balance
being paid down (the account or the bill).

```sql
-- before moving money
UPDATE account SET pending_tx_id = 101
WHERE id = 7 AND pending_tx_id IS NULL;
-- 1 row: proceed.  0 rows: "a payment is still in progress"
```

- Store the transaction ID, not a flag. It shows who holds it and makes a
  safe release possible.
- Release only when the transaction is **final** (posted, or failure
  confirmed), not when the response is sent. An in-doubt payment keeps the
  block until reconciliation settles it. That's the window it exists for.
- Release conditionally: `... SET pending_tx_id = NULL WHERE id = 7 AND
  pending_tx_id = 101`, so you never clear someone else's.
- This claim has no expiry, so the recovery job / reconciliation / person
  chain above is mandatory. Without it the account is blocked forever.

A distributed lock can't hold this block on its own: it's released when
the request ends, and the in-doubt state outlives it (see
[distributed lock](./distributed-lock.md#do-you-need-a-lock)).

---

## When to Use What

1. Can the receiver reject a repeated key? Use it. Done.
2. If not, carry your own ID on every call, and find out what finality the
   receiver offers.
3. Compare the cost of a duplicate with the cost of a delay. If the
   duplicate is worse, don't auto-retry; wait for finality.
4. Reconcile on a schedule, whatever else you do.
5. Once money has moved, finish forward, with the value date.
6. Block dependent requests on the shared row while anything is in doubt.

---

## Related

- [transactional outbox](./transactional-outbox.md): at-least-once on the
  producer side, with an `event_id` fixed at insert time.
- [API idempotency](./api-idempotency.md): the same idea when **you** are
  the receiver.
- [distributed lock](./distributed-lock.md): why the guarantee comes from
  the resource's check, not the lock.
- [defining the problem](./defining-the-problem.md): "exactly-once
  delivery" is the ask; the effect is what's needed.
