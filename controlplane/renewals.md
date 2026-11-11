# Funded Google renewal

`Client.RenewEntitlement` calls the `renewal_url` in a verified managed Google
realtime SessionPlan. The endpoint must use the configured HTTPS cell origin.
It sends the plan's `gwt1.` token, protocol revision 3, an explicit idempotency
key, and only `previous_expires_at`. Redirects are refused.

The cell decides the renewal window from its database clock (currently the
last 30 seconds of the funded slice). A caller can request renewal 15 seconds
before expiry. A lost acknowledgment is retried with exactly the same previous
expiry and key; changing either can fork the caller's intent and is refused by
the cell. This method performs one HTTP request per invocation.

The response binds the session, attempt, reservation and previous expiry. Its
new horizon is at most 300 seconds later and never beyond the signed original
`renewable_until`. The bearer credential expires at that new horizon. Numeric
int64 fields retain exact values; the Gateway does not calculate money.

This API does not adopt the credential into an adapter or extend an Engine
session. Runtime lease adoption and Google reconnect/resumption are separate
work. An application must not advance a running lease merely because this
method returned a funded credential.
