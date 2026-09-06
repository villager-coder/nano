# Runtime lifecycle and concurrency

## Task submission

`scheduler.PushTask(task)` now returns an error and never blocks.
Handle `scheduler.ErrQueueFull` and `scheduler.ErrClosed`; ignoring the return
value can lose work. The built-in handler dispatch propagates admission errors:
a TCP/WebSocket client is disconnected on a failed dispatch, and an RPC caller
receives an error. Existing custom `LocalScheduler` implementations retain their
`Schedule(Task)` interface and are responsible for their own overload policy.

Connection close hooks use a separate finalizer queue. Socket closure and local
session removal happen before close hooks or remote notifications. Finalizers run
on the shared logic scheduler, including when its business queue is full.

## Request sessions

A handler receives a request view of its session. Its ID, UID, data and router are
shared with the connection, while its response MID is immutable. Consequently,
`s.Response(payload)` inside an asynchronous callback responds to the request
that created that view, even after later requests have run.

Use `s.ID()` for identity rather than comparing session pointers. Use
`s.Connection()` when retaining a session for work unrelated to the current
request. `s.Push(...)` always uses the connection lifetime, so room broadcasts
continue after an individual request expires.

`s.Context()` exposes the request context. `s.RPCContext(ctx, route, payload)`
allows an explicit cancellation/deadline; the built-in agent and acceptor
propagate it. Third-party NetworkEntity implementations can opt in by
implementing RPCContext, PushContext and ResponseMidContext, without changing
the existing NetworkEntity interface.

State() returns a shallow map snapshot, and Restore() copies the incoming map.
Nested mutable values still require application synchronization. Raw inbound
byte payloads are copied before being handed to a queued handler.

## Timeouts and RPC admission

Defaults are 5 seconds for a cluster RPC, 5 seconds for a socket write, and
10 seconds for node shutdown network/task waits. Configure them through
WithRPCTimeout, WithWriteTimeout, WithShutdownTimeout, or the corresponding
cluster.Options fields. A shorter caller deadline takes precedence.

HandleRequest and HandleNotify acknowledge queue admission, not completion.
After that acknowledgment their gRPC transport context ends. Accepted work
retains the original deadline and values, and is canceled when its session or
node closes. Cancellation after acknowledgment requires an application-level
protocol if the caller needs to abort work already admitted. Application RPCs
do not retry automatically, avoiding duplicate game state mutations.

## Startup and shutdown

Startup binds sockets before returning success. StartupContext can be canceled
while registration retries; Shutdown also cancels registration in progress.
Register all application configuration before startup.

Node.Shutdown is idempotent. It stops client admission, closes TCP and upgraded
WebSocket sessions, stops cluster heartbeats/RPC serving, releases client pools,
and waits for connection workers and accepted tasks/finalizers. Network and task
waits share the shutdown timeout. Component initialization/shutdown hooks and
custom schedulers must cooperate: Go cannot forcibly terminate application
callbacks. The framework does not guarantee delivery of queued outbound messages
once a connection closes.

For direct cluster.Node use, run scheduler.Sched() and keep it running until all
nodes finish Shutdown; then call scheduler.Close(). Scheduler.Close drains
accepted work and must be called outside a scheduler callback. Nodes and the
process-wide scheduler cannot be restarted after shutdown; create a new process
for a complete application restart.
