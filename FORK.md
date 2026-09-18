# This fork

`github.com/ipni/go-libp2p-kad-dht` is IPNI's fork of
[`libp2p/go-libp2p-kad-dht`](https://github.com/libp2p/go-libp2p-kad-dht). It exists to
carry a small set of changes to the accelerated DHT client (`fullrt`) that
[someguy](https://github.com/ipni/someguy), the delegated routing service behind
`route-*.ipni.io`, needs and upstream does not have. Nothing else is changed.

The module path is unchanged: this is still `github.com/libp2p/go-libp2p-kad-dht`
inside `go.mod`, so a consumer uses a `replace` directive rather than a new import
path:

```
replace github.com/libp2p/go-libp2p-kad-dht => github.com/ipni/go-libp2p-kad-dht v0.42.2-ipni.2
```

## Why a fork and not pull requests

The changes are correct and small, but upstream review capacity in 2026 is one
maintainer on a monthly cadence, and the arguments for each change take longer to
make than the change took to write. Every patch here is either a new option whose
default reproduces upstream behaviour, or a behaviour change to one function
(`FullRT.FindPeer`) that has options to restore the old timing. That keeps the diff
against each upstream release confined to `fullrt/dht.go`, `fullrt/options.go`,
their tests and `version.json`, so rebasing is cheap and arguing is optional. If any
of these is ever upstreamed, drop the patch here and the `replace` disappears.

## Branch, tags, rebase

- Development branch: `fix/fullrt-findpeer`, always rebased onto the newest upstream
  release tag, never merged with it. `master` tracks upstream `master` untouched.
- Tags: `vX.Y.Z-ipni.N`, where `vX.Y.Z` is the upstream release the branch sits on and
  `N` counts fork releases on that base. `version.json` matches the tag.
- On each upstream release: `git fetch upstream --tags`, rebase the branch onto the new
  tag, `gofmt -s -l .` must be empty, `go vet ./...`, `go test -race -count=2
  ./fullrt/...`, `go test ./...` once (the integration tests are slow), bump
  `version.json`, tag, push branch and tag, then bump the `replace` in someguy and let
  its CI build. Half an hour when the rebase applies clean; when it does not, the
  conflict is in one of the three files above and the section for that patch below
  says what the code must still do.
- Verify a tag resolves before pointing anything at it:
  `go list -m github.com/libp2p/go-libp2p-kad-dht@vX.Y.Z-ipni.N` from a scratch module
  carrying the `replace`.

## The patches

All measurements below were taken on IPNI's `sing-1` box (Singapore) in September
2026 with someguy serving a replay of real cid.contact traffic at 350 requests per
second. The full reports live in the IPNI operators' `someguy-profiles` archive; the
numbers that justify each patch are repeated here so this file stands alone.

### 1. `FindPeer` returns the addresses the network reported

**Upstream behaviour.** After querying the 20 closest servers for the target,
`FullRT.FindPeer` dials the target with a hardcoded 5 s timeout when any server
reported its addresses, and returns a result only if that dial produced a
connection. Otherwise `ErrNotFound`, and the reported addresses are discarded.

**Problem.** For a NATed, firewalled or offline peer the dial always fails, so a
correct answer is thrown away after a 5 s wait. The standard client does not do
this: `IpfsDHT.FindPeer` returns a peer's addresses whenever the query dialed it,
failed dial or not (`dialedPeerDuringQuery` in `routing.go`). A caller who moves to
the accelerated client silently loses answers, and a delegated router's clients dial
for themselves anyway.

**Change.** After the query: connected, return the peerstore's info as before;
nobody reported the target, `ErrNotFound` as before; otherwise add the reported
addresses to the peerstore with `TempAddrTTL`, start the dial in the background so
identify can refine the addresses for later callers, and return the reported
addresses at once.

**Measured.** 42,178 peers lookups per run: mean response size rose from 62 to 71
bytes as addresses that were being discarded were returned. Latency did not move
with this patch alone; the 5 s those lookups took turned out to be request queueing
in the caller (see "Not patched" below) and was fixed in someguy.

**Tests.** `TestFindPeerReturnsReportedAddrsWhenUnreachable`, `TestFindPeerNotFound`,
`TestFindPeerPrefersConnectedPeer`, `TestFindPeerSelfReturnsNotFound`,
`TestFindPeerCallerCancelledSurfacesError`, `TestFindPeerDialOutlivesCallerContext`.

**Drop when** upstream `FindPeer` stops gating its answer on the dial.

### 2. `FindPeer` stops querying shortly after the first report

**Upstream behaviour.** `FindPeer` runs `execOnMany` with `sloppyExit=false`, so the
query runs until every closest server answers or errors, or `timeoutPerOp` (5 s)
fires. Right for `GET_PROVIDERS`, where servers hold different records; wasteful for
`FIND_NODE` on one peer, where the other nineteen report the same record or nothing
and the slowest decides when the caller gets an answer.

**Change.** On the first report of the target, start a grace timer (default 500 ms).
When it elapses, cancel the query; servers that answer inside the grace still
contribute. Lookups where nobody reports the target are unchanged.

**Trade-off.** An address known only to a server that answers more than the grace
after the first report is dropped. Such reports were rarely different from the
first, and the background dial from patch 1 can still pick the address up.

**Tests.** `TestFindPeerCutsQueryAfterGrace`, `TestFindPeerGraceDisabled`,
`TestFindPeerGraceDropsPostDeadlineReports`.

### 3. Options for patches 1 and 2, and a bound on the background dial

New options in `fullrt/options.go`, in the existing style:

- `WithFindPeerGrace(d)`: the grace from patch 2. Default 500 ms. `0` disables the
  early exit and waits for the full query, the pre-fork wait.
- `WithFindPeerDialTimeout(d)`: budget for the background dial from patch 1. Default
  5 s, the old hardcoded value. `0` skips the dial; a caller with its own address
  cache (someguy) gains nothing from identify refinement.
- `WithMaxConcurrentFindPeerDials(n)`: bound on background dials in flight. Default
  64. When the bound is reached a new dial is skipped, not queued: the answer has
  already been returned and nothing waits on the dial. Without this, someguy's
  roughly 100 peers lookups per second would start 100 dials per second, mostly to
  unreachable targets, in the same dial limiter the DHT's own queries use. The swarm
  already dedupes concurrent dials to one peer, so no per-target logic is needed.

**Tests.** `TestFindPeerDialTimeoutDisabled`, `TestFindPeerDialBound`.

### 4. `WithRouteTableFilter`

**Upstream behaviour.** `runCrawler` passes every peer the crawler reports through
`kaddht.PublicRoutingTableFilter`, hardcoded. That filter first rejects any peer the
host has no open connection to, which a live crawl passes because it has just dialed
the peer.

**Problem.** Any crawler that reports peers it did not dial cannot populate the
table. someguy persists the accelerated client's routing table to disk and replays
it at startup through a `crawler.Crawler` wrapper (`fullrt.WithCrawler`); with the
hardcoded filter every replayed peer was rejected, the table came up empty, and the
follow-up refresh was seeded with nothing. The standard client already exposes
`kaddht.RoutingTableFilter(f)`; fullrt was the outlier.

**Change.** A filter field on fullrt's config, `WithRouteTableFilter(f
kaddht.RouteTableFilterFunc)`, default `kaddht.PublicRoutingTableFilter` so nothing
changes for callers who do not set it, and `runCrawler` calling the configured
filter. The filter receives the `*FullRT`, so a caller can delegate to the default
and widen it. someguy's filter applies the default when the peer has connections
and, when it has none, accepts if the host peerstore holds at least one public
address; private peers still never enter the table.

**Measured.** Accelerated client ready 11.4 s after process start against a 95 to
164 s cold crawl.

**Tests.** `TestRouteTableFilterDefaultDropsUnconnectedPeers`,
`TestRouteTableFilterSuppliedFilterIsHonoured`, `TestRouteTableFilterRejectAll`,
`TestWithRouteTableFilterRejectsNil`.

**Drop when** upstream fullrt gains an equivalent option. This is the one patch
worth sending upstream if anyone ever has the afternoon: ten lines, a parity
argument, no behaviour change.

### 5. The `addrsCh` drain race (introduced by patch 2, fixed before tagging)

Upstream's `fn` sent each report with `select { case addrsCh <- a: case
<-ctx.Done(): }` on a channel of size 1. Safe with one cancel source; patch 2 added a
second (the grace timer), and with both select cases ready Go picks at random, so
about half the parsed reports were dropped at the deadline. The channel is now sized
to `len(peers)` (each `fn` sends at most once, so a send never blocks) and the send has
no select. `close(addrsCh)` runs only after `execOnMany` returns, which waits for
every `fn`, so no send can follow the close. Anyone touching the fan-out again must
keep both invariants.

**Test.** `TestFindPeerGraceCancelRace` (run with `-race`).

## Not patched, on purpose

Two findings from the same measurements point at kad-dht but are handled elsewhere
or left alone. They are recorded here so nobody re-derives them.

**Per-peer request serialisation.** `internal/net` sends all requests to a given peer
over one stream, one at a time (`peerMessageSender` and its lock). Many concurrent
lookups for the same key all query the same 20 servers and queue behind each other
per server; under sustained load the queue never drains and each request waits out
`timeoutPerOp`. Measured: four peer IDs that were 69.5% of peers traffic ran at
5,000 ms medians while identical idle lookups took 170 ms, and thousands of
goroutines sat on the per-peer lock in every mutex profile. The right layer for the
fix is the caller: someguy deduplicates concurrent lookups per target
(singleflight) and caches not-found for a short TTL, which took those IDs to 0.2 ms
under load and cut goroutines under load from 28,558 to 15,081. Changing the
one-stream design inside kad-dht is a maintainers' decision and is not carried
here.

**Slow closest servers on not-found lookups.** For a target nobody reports,
`execOnMany` with `sloppyExit=false` waits for every closest server or the 5 s cap,
so one slow server in the closest set costs the full 5 s on every not-found lookup
for that key. `findProvidersAsyncRoutine` has the same exposure. One of the four hot
IDs above stayed at 1.7 s median after deduplication for this reason; someguy's
negative cache covers it. A `sloppyExit` path for not-found or a lower cap for
`FIND_NODE` would bound it inside kad-dht, but either changes what not-found means
and has not been done.

## Consumers

- `github.com/ipni/someguy`: `replace` in `go.mod`, options wired in
  `server_dht.go`, exposed as `SOMEGUY_DHT_FIND_PEER_GRACE`,
  `SOMEGUY_DHT_FIND_PEER_DIAL_TIMEOUT` and the crawl snapshot's filter.
