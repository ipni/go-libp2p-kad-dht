package fullrt

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/crawler"
	dht_pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p-kad-dht/records"
	record "github.com/libp2p/go-libp2p-record"
	kadkey "github.com/libp2p/go-libp2p-xor/key"
	"github.com/libp2p/go-libp2p-xor/trie"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDivideByChunkSize(t *testing.T) {
	var keys []peer.ID
	for i := range 10 {
		keys = append(keys, peer.ID(strconv.Itoa(i)))
	}

	convertToStrings := func(peers []peer.ID) []string {
		var out []string
		for _, p := range peers {
			out = append(out, string(p))
		}
		return out
	}

	pidsEquals := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i, v := range a {
			if v != b[i] {
				return false
			}
		}
		return true
	}

	t.Run("Divides", func(t *testing.T) {
		gr := divideByChunkSize(keys, 5)
		if len(gr) != 2 {
			t.Fatal("incorrect number of groups")
		}
		if g1, expected := convertToStrings(gr[0]), []string{"0", "1", "2", "3", "4"}; !pidsEquals(g1, expected) {
			t.Fatalf("expected %v, got %v", expected, g1)
		}
		if g2, expected := convertToStrings(gr[1]), []string{"5", "6", "7", "8", "9"}; !pidsEquals(g2, expected) {
			t.Fatalf("expected %v, got %v", expected, g2)
		}
	})
	t.Run("Remainder", func(t *testing.T) {
		gr := divideByChunkSize(keys, 3)
		if len(gr) != 4 {
			t.Fatal("incorrect number of groups")
		}
		if g, expected := convertToStrings(gr[0]), []string{"0", "1", "2"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
		if g, expected := convertToStrings(gr[1]), []string{"3", "4", "5"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
		if g, expected := convertToStrings(gr[2]), []string{"6", "7", "8"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
		if g, expected := convertToStrings(gr[3]), []string{"9"}; !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
	})
	t.Run("OneEach", func(t *testing.T) {
		gr := divideByChunkSize(keys, 1)
		if len(gr) != 10 {
			t.Fatal("incorrect number of groups")
		}
		for i := range 10 {
			if g, expected := convertToStrings(gr[i]), []string{strconv.Itoa(i)}; !pidsEquals(g, expected) {
				t.Fatalf("expected %v, got %v", expected, g)
			}
		}
	})
	t.Run("ChunkSizeLargerThanKeys", func(t *testing.T) {
		gr := divideByChunkSize(keys, 11)
		if len(gr) != 1 {
			t.Fatal("incorrect number of groups")
		}
		if g, expected := convertToStrings(gr[0]), convertToStrings(keys); !pidsEquals(g, expected) {
			t.Fatalf("expected %v, got %v", expected, g)
		}
	})
}

func TestIPDiversityFilter(t *testing.T) {
	ctx := context.Background()
	h, err := libp2p.New()
	require.NoError(t, err)
	dht, err := NewFullRT(h, "", DHTOption(kaddht.BootstrapPeers(kaddht.GetDefaultBootstrapPeerAddrInfos()...)))
	require.NoError(t, err)

	dht.bucketSize = 3
	dht.ipDiversityFilterLimit = 1

	// peer id whose kadid starts with 15 0's
	target, err := peer.Decode("QmNLfyis4M4iAWth8ApJwbCfuQaaaXKWECGAHQfXKUG6C7")
	require.NoError(t, err)

	type addr struct {
		ipv6 bool
		addr string
	}
	// setDhtPeers replaces the dht's routing table with the provided addresses
	// assigned with random new peer ids. The provided order of addresses is also
	// the kademlia distance order to the key requested later.
	setDhtPeers := func(peerMaddrs ...[]addr) []peer.ID {
		newTrie := trie.New()
		peerAddrs := make(map[peer.ID][]ma.Multiaddr)
		kPeerMap := make(map[string]peer.ID)
		pids := make([]peer.ID, 0, len(peerMaddrs))
		for i, ips := range peerMaddrs {
			_, pubKey, err := crypto.GenerateEd25519Key(rand.Reader)
			require.NoError(t, err)
			pid, err := peer.IDFromPublicKey(pubKey)
			require.NoError(t, err)
			p := &peer.AddrInfo{ID: pid, Addrs: make([]ma.Multiaddr, 0, len(ips))}
			for _, ip := range ips {
				var a ma.Multiaddr
				var err error
				if ip.ipv6 {
					a, err = ma.NewMultiaddr("/ip6/" + ip.addr + "/tcp/4001")
				} else {
					a, err = ma.NewMultiaddr("/ip4/" + ip.addr + "/tcp/4001")
				}
				require.NoError(t, err)
				p.Addrs = append(p.Addrs, a)
			}
			k := [32]byte{}
			k[0] = byte(i)
			kadKey := kadkey.Key(k[:])
			_, ok := newTrie.Add(kadKey)
			require.True(t, ok)
			kPeerMap[string(kadKey)] = p.ID
			peerAddrs[p.ID] = p.Addrs
			pids = append(pids, p.ID)
		}
		dht.rt = newTrie
		dht.peerAddrsLk.Lock()
		dht.peerAddrs = peerAddrs
		dht.peerAddrsLk.Unlock()
		dht.kMapLk.Lock()
		dht.keyToPeerMap = kPeerMap
		dht.kMapLk.Unlock()
		return pids
	}

	t.Run("Different IPv4 blocks", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}},
			{{ipv6: false, addr: "2.2.2.2"}},
			{{ipv6: false, addr: "3.3.3.3"}},
			{{ipv6: false, addr: "4.4.4.4"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Equal(t, pids[:dht.bucketSize], cp)
	})

	t.Run("Duplicate address from IPv4 block", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}},
			{{ipv6: false, addr: "1.1.2.2"}},
			{{ipv6: false, addr: "3.3.3.3"}},
			{{ipv6: false, addr: "4.4.4.4"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Contains(t, cp, pids[0])
		require.Contains(t, cp, pids[2])
		require.Contains(t, cp, pids[3])
	})

	t.Run("Duplicate address from 2 IPv4 blocks", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}},
			{{ipv6: false, addr: "1.1.2.2"}},
			{{ipv6: false, addr: "3.3.3.3"}},
			{{ipv6: false, addr: "3.3.4.4"}},
			{{ipv6: false, addr: "5.5.5.5"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Contains(t, cp, pids[0])
		require.Contains(t, cp, pids[2])
		require.Contains(t, cp, pids[4])
	})

	t.Run("Different IPv6 blocks", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: true, addr: "2001:4860:4860::1"}},
			{{ipv6: true, addr: "2606:4700:4700::2"}},
			{{ipv6: true, addr: "2620:fe::3"}},
			{{ipv6: true, addr: "2a02:6b8::4"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Equal(t, pids[:dht.bucketSize], cp)
	})

	t.Run("Duplicate address from IPv6 block", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: true, addr: "2001:4860:4860::1"}},
			{{ipv6: true, addr: "2001:4860:4860::2"}},
			{{ipv6: true, addr: "2620:fe::3"}},
			{{ipv6: true, addr: "2a02:6b8::4"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Contains(t, cp, pids[0])
		require.Contains(t, cp, pids[2])
		require.Contains(t, cp, pids[3])
	})

	t.Run("Duplicate address from 2 IPv6 blocks", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: true, addr: "2001:4860:4860::1"}},
			{{ipv6: true, addr: "2001:4860:4860::2"}},
			{{ipv6: true, addr: "2606:4700:4700::3"}},
			{{ipv6: true, addr: "2606:4700:4700::4"}},
			{{ipv6: true, addr: "2620:fe::5"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Contains(t, cp, pids[0])
		require.Contains(t, cp, pids[2])
		require.Contains(t, cp, pids[4])
	})

	t.Run("IPv4+IPv6 acceptable representation", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}, {ipv6: true, addr: "2001:4860:4860::1"}},
			{{ipv6: false, addr: "2.2.2.2"}, {ipv6: true, addr: "2606:4700:4700::2"}},
			{{ipv6: false, addr: "3.3.3.3"}, {ipv6: true, addr: "2620:fe::3"}},
			{{ipv6: false, addr: "4.4.4.4"}, {ipv6: true, addr: "2a02:6b8::4"}},
			{{ipv6: false, addr: "5.5.5.5"}, {ipv6: true, addr: "2620:fe::5"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Equal(t, pids[:dht.bucketSize], cp)
	})

	t.Run("IPv4+IPv6 overrepresentation", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}, {ipv6: true, addr: "2001:4860:4860::1"}},
			{{ipv6: false, addr: "1.1.2.2"}, {ipv6: true, addr: "2606:4700:4700::2"}},
			{{ipv6: false, addr: "3.3.3.3"}, {ipv6: true, addr: "2606:4700:4700::3"}},
			{{ipv6: false, addr: "4.4.4.4"}, {ipv6: true, addr: "2001:4860:4860::4"}},
			{{ipv6: false, addr: "5.5.5.5"}, {ipv6: true, addr: "2620:fe::5"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Contains(t, cp, pids[0])
		require.Contains(t, cp, pids[2])
		require.Contains(t, cp, pids[4])
	})

	dht.ipDiversityFilterLimit = 0
	t.Run("Disabled IP Diversity Filter", func(t *testing.T) {
		pids := setDhtPeers([][]addr{
			{{ipv6: false, addr: "1.1.1.1"}, {ipv6: true, addr: "2606:4700:4700::1"}},
			{{ipv6: false, addr: "1.1.2.2"}, {ipv6: true, addr: "2606:4700:4700::2"}},
			{{ipv6: false, addr: "1.1.3.3"}, {ipv6: true, addr: "2606:4700:4700::3"}},
			{{ipv6: false, addr: "1.1.4.4"}, {ipv6: true, addr: "2606:4700:4700::4"}},
			{{ipv6: false, addr: "1.1.5.5"}, {ipv6: true, addr: "2606:4700:4700::5"}},
		}...)
		cp, err := dht.GetClosestPeers(ctx, string(target))
		require.NoError(t, err)
		require.Len(t, cp, dht.bucketSize)
		require.Equal(t, pids[:dht.bucketSize], cp)
	})
}

// blockingCrawler parks inside Run until the context is cancelled. runCrawler
// only rebuilds the routing table after Run returns, so a test that hand-installs
// a routing table cannot have it overwritten by the crawl that NewFullRT starts.
type blockingCrawler struct{}

var _ crawler.Crawler = blockingCrawler{}

func (blockingCrawler) Run(ctx context.Context, _ []*peer.AddrInfo, _ crawler.HandleQueryResult, _ crawler.HandleQueryFail) {
	<-ctx.Done()
}

// newTestFullRT builds a FullRT that neither crawls nor dials. It uses the empty
// protocol prefix, which skips Config.Validate, so callers may disable
// subsystems. BootstrapPeers is passed explicitly because NewFullRT calls the
// config's BootstrapPeers func unconditionally and nothing else sets it.
func newTestFullRT(t *testing.T, opts ...Option) *FullRT {
	t.Helper()

	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Close()) })

	base := []Option{
		WithCrawler(blockingCrawler{}),
		DHTOption(kaddht.BootstrapPeers()),
	}
	frt, err := NewFullRT(h, "", append(base, opts...)...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, frt.Close()) })

	// The hand-rolled config in NewFullRT never sets BucketSize, so GetClosestPeers
	// would otherwise return no peers at all.
	frt.bucketSize = 1
	return frt
}

// setTriePeers replaces the routing table with exactly the given peers, none of
// which carry addresses. GetClosestPeers then returns them without consulting
// the IP diversity filter and without dialing.
func setTriePeers(t *testing.T, frt *FullRT, pids ...peer.ID) {
	t.Helper()

	newTrie := trie.New()
	kPeerMap := make(map[string]peer.ID, len(pids))
	for i, pid := range pids {
		k := [32]byte{}
		k[0] = byte(i)
		kadKey := kadkey.Key(k[:])
		_, ok := newTrie.Add(kadKey)
		require.True(t, ok)
		kPeerMap[string(kadKey)] = pid
	}

	frt.rtLk.Lock()
	frt.rt = newTrie
	frt.rtLk.Unlock()

	frt.kMapLk.Lock()
	frt.keyToPeerMap = kPeerMap
	frt.kMapLk.Unlock()

	frt.peerAddrsLk.Lock()
	frt.peerAddrs = make(map[peer.ID][]ma.Multiaddr, len(pids))
	frt.peerAddrsLk.Unlock()
}

// blankValidator accepts everything, so a test can store records under any
// namespace without building a real validator.
type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

// testMessageSender is a local copy of the fake sender in package dht, which is
// package-private there.
type testMessageSender struct {
	sendRequest func(ctx context.Context, p peer.ID, pmes *dht_pb.Message) (*dht_pb.Message, error)
	sendMessage func(ctx context.Context, p peer.ID, pmes *dht_pb.Message) error
}

var _ dht_pb.MessageSender = (*testMessageSender)(nil)

func (t testMessageSender) SendRequest(ctx context.Context, p peer.ID, pmes *dht_pb.Message) (*dht_pb.Message, error) {
	return t.sendRequest(ctx, p, pmes)
}

func (t testMessageSender) SendMessage(ctx context.Context, p peer.ID, pmes *dht_pb.Message) error {
	return t.sendMessage(ctx, p, pmes)
}

// TestFindProvidersAsyncShufflesRemoteProviders verifies the receive-side
// reorder: providers returned by a queried peer are shuffled before the
// count-capped selection loop, so the subset we keep and surface is spread
// across the returned providers rather than always the first ones listed. A
// deterministic reversing shuffle makes the assertion exact and also proves the
// shuffle runs BEFORE the count cap: with a cap of three, we must keep the
// reversed prefix (the tail of the returned ordering), not its first three
// entries.
//
// The routing table holds exactly one peer so that execOnMany, which queries
// every peer concurrently, runs the shuffle on a single goroutine.
func TestFindProvidersAsyncShufflesRemoteProviders(t *testing.T) {
	frt := newTestFullRT(t)

	remote := make([]peer.AddrInfo, 16)
	for i := range remote {
		remote[i] = peer.AddrInfo{ID: peer.ID(fmt.Sprintf("provider-%02d", i))}
	}

	_, pubKey, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	responder, err := peer.IDFromPublicKey(pubKey)
	require.NoError(t, err)
	setTriePeers(t, frt, responder)

	// The single queried peer always answers GET_PROVIDERS with the same fixed
	// order, so the shuffle runs exactly once and deterministically.
	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(_ context.Context, _ peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_GET_PROVIDERS, req.Type)
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			resp.ProviderPeers = dht_pb.RawPeerInfosToPBPeers(remote)
			return resp, nil
		},
	})
	require.NoError(t, err)

	// A reversing shuffle stands in for the injected rand source. It must not be
	// a globally seeded *rand.Rand: execOnMany may call this concurrently.
	frt.shuffle = func(k int, swap func(i, j int)) {
		for i := range k / 2 {
			swap(i, k-1-i)
		}
	}

	mhash, err := multihash.Sum([]byte("shuffle-me"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	key := cid.NewCidV1(cid.Raw, mhash)

	var got []peer.ID
	for ai := range frt.FindProvidersAsync(t.Context(), key, 3) {
		got = append(got, ai.ID)
	}

	want := []peer.ID{peer.ID("provider-15"), peer.ID("provider-14"), peer.ID("provider-13")}
	require.Equal(t, want, got)
}

func testCid(t *testing.T) cid.Cid {
	t.Helper()
	mhash, err := multihash.Sum([]byte("store-presence"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mhash)
}

// TestCloseWithSubsystemsDisabled pins the nil-store contract at its sharpest
// edge: a FullRT that constructed neither store must still close cleanly.
func TestCloseWithSubsystemsDisabled(t *testing.T) {
	frt := newTestFullRT(t, DHTOption(kaddht.DisableProviders(), kaddht.DisableValues()))

	require.Nil(t, frt.ProviderManager)
	require.Nil(t, frt.valueStore)
	// Close runs via t.Cleanup, which fails the test if it errors or panics.
}

// TestGatesOnStorePresence verifies that an absent store makes its RPCs report
// unsupported, rather than the removed enableValues/enableProviders bools doing so.
func TestGatesOnStorePresence(t *testing.T) {
	key := testCid(t)

	t.Run("values disabled", func(t *testing.T) {
		frt := newTestFullRT(t, DHTOption(kaddht.DisableValues()))
		require.Nil(t, frt.valueStore)
		require.NotNil(t, frt.ProviderManager)

		require.ErrorIs(t, frt.PutValue(t.Context(), "/pk/k", []byte("v")), routing.ErrNotSupported)
		_, err := frt.GetValue(t.Context(), "/pk/k")
		require.ErrorIs(t, err, routing.ErrNotSupported)
		_, err = frt.SearchValue(t.Context(), "/pk/k")
		require.ErrorIs(t, err, routing.ErrNotSupported)
		require.ErrorIs(t, frt.PutMany(t.Context(), []string{"/pk/k"}, [][]byte{[]byte("v")}), routing.ErrNotSupported)
	})

	t.Run("providers disabled", func(t *testing.T) {
		frt := newTestFullRT(t, DHTOption(kaddht.DisableProviders()))
		require.Nil(t, frt.ProviderManager)
		require.NotNil(t, frt.valueStore)

		require.ErrorIs(t, frt.Provide(t.Context(), key, true), routing.ErrNotSupported)
		require.ErrorIs(t, frt.ProvideMany(t.Context(), []multihash.Multihash{key.Hash()}), routing.ErrNotSupported)
		_, err := frt.FindProviders(t.Context(), key)
		require.ErrorIs(t, err, routing.ErrNotSupported)

		ch := frt.FindProvidersAsync(t.Context(), key, 1)
		_, ok := <-ch
		require.False(t, ok, "FindProvidersAsync must return a closed, empty channel")
	})
}

// TestDefaultPrefixRequiresValueAndProviderStores documents why nil stores stay
// unreachable on the Amino DHT: Validate rejects disabling either subsystem, so
// only a fork on another prefix ever sees an absent store.
func TestDefaultPrefixRequiresValueAndProviderStores(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  kaddht.Option
		want string
	}{
		{"providers", kaddht.DisableProviders(), "must have providers enabled"},
		{"values", kaddht.DisableValues(), "must have values enabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, err := libp2p.New(libp2p.NoListenAddrs)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, h.Close()) })

			_, err = NewFullRT(h, amino.ProtocolPrefix, WithCrawler(blockingCrawler{}),
				DHTOption(kaddht.BootstrapPeers(), kaddht.BucketSize(amino.DefaultBucketSize), tc.opt))
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func recordKeys(t *testing.T, ctx context.Context, dstore ds.Datastore) []string {
	t.Helper()
	res, err := dstore.Query(ctx, dsq.Query{KeysOnly: true})
	require.NoError(t, err)
	defer res.Close()

	var keys []string
	for e := range res.Next() {
		require.NoError(t, e.Error)
		keys = append(keys, e.Key)
	}
	return keys
}

// TestValueDatastoreOverride checks that fullrt honors ValueDatastore, sending
// value records to the override while the main datastore stays empty.
func TestValueDatastoreOverride(t *testing.T) {
	ctx := t.Context()
	main := dssync.MutexWrap(ds.NewMapDatastore())
	values := dssync.MutexWrap(ds.NewMapDatastore())

	frt := newTestFullRT(t, DHTOption(
		kaddht.Datastore(main),
		kaddht.ValueDatastore(values),
		kaddht.Validator(blankValidator{}),
	))

	key := "/v/somekey"
	require.NoError(t, frt.valueStore.Put(ctx, key, record.MakePutRecord(key, []byte("hello"))))

	got, err := frt.valueStore.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), got.GetValue())

	require.NotEmpty(t, recordKeys(t, ctx, values))
	require.Empty(t, recordKeys(t, ctx, main))
}

// TestProviderDatastoreOverride checks that fullrt honors ProviderDatastore,
// sending provider records to the override while the main datastore stays empty.
func TestProviderDatastoreOverride(t *testing.T) {
	ctx := t.Context()
	main := dssync.MutexWrap(ds.NewMapDatastore())
	providers := dssync.MutexWrap(ds.NewMapDatastore())

	frt := newTestFullRT(t, DHTOption(
		kaddht.Datastore(main),
		kaddht.ProviderDatastore(providers),
	))

	key := []byte("provider-override-key")
	frt.ProviderManager.AddProvider(ctx, key, peer.AddrInfo{ID: frt.self})

	provs, err := frt.ProviderManager.GetProviders(ctx, key)
	require.NoError(t, err)
	require.Len(t, provs, 1)

	require.NoError(t, frt.ProviderManager.Close())
	require.NotEmpty(t, recordKeys(t, ctx, providers))
	require.Empty(t, recordKeys(t, ctx, main))
}

// TestProviderManagerOptsReachTheManager checks that provider manager options
// routed through DHTOption are applied, and that fullrt's own
// WithProviderManagerOptions still wins on conflict because it is applied last.
//
// ProvideValidity is asserted through behavior: a negative validity expires a
// provider record the moment it is written, so GetProviders serves nothing. A
// tiny positive validity would not do. The record round-trips through the
// datastore, which drops the monotonic reading, so expiry compares the wall
// clocks of the write and the read — and Windows advances its wall clock only
// every ~15ms, making both readings identical and the record still valid.
func TestProviderManagerOptsReachTheManager(t *testing.T) {
	addAndGet := func(t *testing.T, frt *FullRT) []peer.AddrInfo {
		t.Helper()
		key := []byte("provider-manager-opts-key")
		frt.ProviderManager.AddProvider(t.Context(), key, peer.AddrInfo{ID: frt.self})
		provs, err := frt.ProviderManager.GetProviders(t.Context(), key)
		require.NoError(t, err)
		return provs
	}

	t.Run("applied from DHTOption", func(t *testing.T) {
		frt := newTestFullRT(t, DHTOption(kaddht.ProviderManagerOpts(records.ProvideValidity(-time.Second))))
		require.Empty(t, addAndGet(t, frt), "records must expire immediately, so the option reached the manager")
	})

	t.Run("fullrt-native option wins", func(t *testing.T) {
		frt := newTestFullRT(t,
			DHTOption(kaddht.ProviderManagerOpts(records.ProvideValidity(-time.Second))),
			WithProviderManagerOptions(records.ProvideValidity(time.Hour)),
		)
		require.Len(t, addAndGet(t, frt), 1, "the fullrt-native validity must override the DHTOption one")
	})
}

// addrStrings renders a slice of multiaddrs as strings so tests can compare
// addresses by value without relying on the internal representation.
func addrStrings(addrs []ma.Multiaddr) []string {
	out := make([]string, len(addrs))
	for i, a := range addrs {
		out[i] = a.String()
	}
	return out
}

// newTestResponder is a peer id that is not the DHT's own id and is distinct
// from any other peer generated by the test.
func newTestResponder(t *testing.T) peer.ID {
	t.Helper()
	_, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id, err := peer.IDFromPublicKey(pub)
	require.NoError(t, err)
	return id
}

// TestFindPeerReturnsReportedAddrsWhenUnreachable is the core of the fix: when
// the closest peers report the target's addresses but the target cannot be
// dialed, FindPeer must still return those addresses rather than ErrNotFound.
// The reported address points at a closed port so even the background dial
// fails fast, isolating the "dial did not produce a connection" path.
func TestFindPeerReturnsReportedAddrsWhenUnreachable(t *testing.T) {
	frt := newTestFullRT(t, WithTimeoutPerOperation(5*time.Second))
	frt.bucketSize = 3
	setTriePeers(t, frt, newTestResponder(t), newTestResponder(t), newTestResponder(t))

	target := newTestResponder(t)
	targetAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1")
	require.NoError(t, err)
	reported := []peer.AddrInfo{{ID: target, Addrs: []ma.Multiaddr{targetAddr}}}

	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(_ context.Context, _ peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_FIND_NODE, req.Type)
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			resp.CloserPeers = dht_pb.RawPeerInfosToPBPeers(reported)
			return resp, nil
		},
	})
	require.NoError(t, err)

	start := time.Now()
	pi, err := frt.FindPeer(t.Context(), target)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, target, pi.ID)
	require.Contains(t, addrStrings(pi.Addrs), targetAddr.String(), "the reported address must be returned even though the dial failed")
	// The answer must not be gated on the dial: well under the per-op timeout.
	require.Less(t, elapsed, 2*time.Second, "FindPeer took %s", elapsed)

	// The address was recorded in the host's peerstore for later callers.
	require.Contains(t, addrStrings(frt.h.Peerstore().Addrs(target)), targetAddr.String())

	// ...but we are not (and were not able to become) connected to it.
	require.False(t, hasValidConnectedness(frt.h, target))
}

// TestFindPeerNotFound keeps the not-found path intact: when the closest peers
// report nothing about the target, FindPeer returns ErrNotFound and does not
// dial, so the target never gains an address in the peerstore.
func TestFindPeerNotFound(t *testing.T) {
	frt := newTestFullRT(t, WithTimeoutPerOperation(5*time.Second))
	frt.bucketSize = 3
	setTriePeers(t, frt, newTestResponder(t), newTestResponder(t), newTestResponder(t))

	target := newTestResponder(t)
	// Unrelated closer peers, each carrying an address, so a naive implementation
	// that dialed whatever it was told would have something to dial.
	other := newTestResponder(t)
	otherAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/2")
	require.NoError(t, err)
	respondWith := []peer.AddrInfo{{ID: other, Addrs: []ma.Multiaddr{otherAddr}}}

	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(_ context.Context, _ peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_FIND_NODE, req.Type)
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			resp.CloserPeers = dht_pb.RawPeerInfosToPBPeers(respondWith)
			return resp, nil
		},
	})
	require.NoError(t, err)

	_, err = frt.FindPeer(t.Context(), target)
	require.ErrorIs(t, err, routing.ErrNotFound)

	// No dial was attempted: the target never gained an address, so there was
	// nothing to dial.
	require.Empty(t, frt.h.Peerstore().Addrs(target))
}

// TestFindPeerPrefersConnectedPeer pins the connectedness shortcut: when we are
// already connected to the target, FindPeer answers locally and does not query
// the network at all.
func TestFindPeerPrefersConnectedPeer(t *testing.T) {
	ctx := t.Context()

	h1, err := libp2p.New()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h1.Close()) })

	h2, err := libp2p.New()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h2.Close()) })

	require.NoError(t, h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}))
	require.True(t, hasValidConnectedness(h1, h2.ID()))

	frt, err := NewFullRT(h1, "", WithCrawler(blockingCrawler{}), DHTOption(kaddht.BootstrapPeers()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, frt.Close()) })
	frt.bucketSize = 1

	var sends atomic.Int32
	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(context.Context, peer.ID, *dht_pb.Message) (*dht_pb.Message, error) {
			sends.Add(1)
			return nil, nil
		},
	})
	require.NoError(t, err)

	pi, err := frt.FindPeer(ctx, h2.ID())
	require.NoError(t, err)
	require.Equal(t, h2.ID(), pi.ID)
	require.Zero(t, sends.Load(), "FindPeer must not query the network when already connected to the target")
}

// TestFindPeerCutsQueryAfterGrace verifies the early exit: once the first peer
// reports the target, FindPeer waits only findPeerGrace (default 500 ms) for
// the near-simultaneous reports from the other close peers and then cancels the
// query, rather than waiting for every one of them.
func TestFindPeerCutsQueryAfterGrace(t *testing.T) {
	frt := newTestFullRT(t, WithTimeoutPerOperation(5*time.Second))

	// The number of responders is deliberately larger than the success threshold
	// execOnMany uses to start its own "good enough" timer (waitFrac of the peer
	// count, 0.3 here): with a single report the heuristic must not fire, so the
	// only thing that can cut the query is the grace window under test. With a
	// regression the slow responders are waited on for the full 5s per-op timeout.
	const numResponders = 8
	frt.bucketSize = numResponders
	responders := make([]peer.ID, numResponders)
	for i := range responders {
		responders[i] = newTestResponder(t)
	}
	setTriePeers(t, frt, responders...)

	target := newTestResponder(t)
	targetAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1")
	require.NoError(t, err)
	reported := []peer.AddrInfo{{ID: target, Addrs: []ma.Multiaddr{targetAddr}}}

	// Exactly one responder reports the target and answers immediately; the rest
	// block until the query context is cancelled.
	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(ctx context.Context, p peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_FIND_NODE, req.Type)
			if p != responders[0] {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			resp.CloserPeers = dht_pb.RawPeerInfosToPBPeers(reported)
			return resp, nil
		},
	})
	require.NoError(t, err)

	start := time.Now()
	pi, err := frt.FindPeer(t.Context(), target)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, target, pi.ID)
	// Fast enough to prove the slow responders were cut (a regression runs the full
	// 5s per-op timeout); slow enough to prove it actually waited for the grace
	// window after the first report rather than returning immediately.
	require.Greater(t, elapsed, 400*time.Millisecond, "FindPeer returned in %s, before the grace window", elapsed)
	require.Less(t, elapsed, 2*time.Second, "FindPeer took %s, the slow responders were not cut", elapsed)
}

// TestFindPeerDialTimeoutDisabled pins WithFindPeerDialTimeout(0): the background
// dial is skipped entirely, so FindPeer never calls Connect for an unreachable
// target. The reported addresses are still returned and recorded in the peerstore;
// only the identify refinement is gone.
func TestFindPeerDialTimeoutDisabled(t *testing.T) {
	frt := newTestFullRT(t, WithTimeoutPerOperation(5*time.Second), WithFindPeerDialTimeout(0))
	frt.bucketSize = 3
	setTriePeers(t, frt, newTestResponder(t), newTestResponder(t), newTestResponder(t))

	target := newTestResponder(t)
	targetAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1")
	require.NoError(t, err)
	reported := []peer.AddrInfo{{ID: target, Addrs: []ma.Multiaddr{targetAddr}}}

	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(_ context.Context, _ peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_FIND_NODE, req.Type)
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			resp.CloserPeers = dht_pb.RawPeerInfosToPBPeers(reported)
			return resp, nil
		},
	})
	require.NoError(t, err)

	pi, err := frt.FindPeer(t.Context(), target)
	require.NoError(t, err)
	require.Equal(t, target, pi.ID)
	require.Contains(t, addrStrings(pi.Addrs), targetAddr.String())

	// No background dial was started, so the host never dialed the target and the
	// only address in the peerstore is the one the query reported.
	require.False(t, hasValidConnectedness(frt.h, target))
	addrs := frt.h.Peerstore().Addrs(target)
	require.Len(t, addrs, 1, "the only address must be the reported one, with no dial having run")
}

// TestFindPeerGraceDisabled pins WithFindPeerGrace(0): there is no early exit after
// the first report, so FindPeer waits for every responder and an address known only
// to a slow peer is still collected. The slow responder answers just under the per-op
// timeout, which also bounds how long this test can take on a regression (the full
// 5s per-op timeout).
func TestFindPeerGraceDisabled(t *testing.T) {
	frt := newTestFullRT(t, WithTimeoutPerOperation(5*time.Second), WithFindPeerGrace(0))

	const numResponders = 8
	frt.bucketSize = numResponders
	responders := make([]peer.ID, numResponders)
	for i := range responders {
		responders[i] = newTestResponder(t)
	}
	setTriePeers(t, frt, responders...)

	target := newTestResponder(t)
	fastAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/3")
	require.NoError(t, err)
	slowAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/4")
	require.NoError(t, err)

	// responders[0] reports the target immediately; responders[1] reports a different
	// address of the target after 3s; the rest block until the query ends. With the
	// default grace the slow report would be dropped and only fastAddr returned.
	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(ctx context.Context, p peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_FIND_NODE, req.Type)
			var addr ma.Multiaddr
			switch p {
			case responders[0]:
				addr = fastAddr
			case responders[1]:
				select {
				case <-time.After(3 * time.Second):
					addr = slowAddr
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			default:
				<-ctx.Done()
				return nil, ctx.Err()
			}
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			resp.CloserPeers = dht_pb.RawPeerInfosToPBPeers([]peer.AddrInfo{{ID: target, Addrs: []ma.Multiaddr{addr}}})
			return resp, nil
		},
	})
	require.NoError(t, err)

	pi, err := frt.FindPeer(t.Context(), target)
	require.NoError(t, err)
	require.Equal(t, target, pi.ID)
	got := addrStrings(pi.Addrs)
	require.Contains(t, got, fastAddr.String())
	require.Contains(t, got, slowAddr.String(), "with no grace window the slow responder's address must be collected")
}

// TestFindPeerDialBound caps the background dials: more FindPeer calls than the bound,
// all for unreachable targets whose dial takes long enough to overlap, must never start
// more concurrent dials than the bound. The host is wrapped so that every Connect attempt
// is recorded and held until the test ends, letting the test observe both how many dials
// started and how many are in flight at once.
func TestFindPeerDialBound(t *testing.T) {
	const n = defaultMaxConcurrentFindPeerDials + 16

	frt := newTestFullRT(t, WithTimeoutPerOperation(5*time.Second), WithFindPeerGrace(0))
	frt.bucketSize = 3

	targets := make([]peer.ID, n)
	for i := range targets {
		targets[i] = newTestResponder(t)
	}
	setTriePeers(t, frt, newTestResponder(t), newTestResponder(t), newTestResponder(t))

	var mu sync.Mutex
	var started, inFlight int32
	release := make(chan struct{})
	defer close(release)
	stub := &recordingHost{
		Host: frt.h,
		connect: func(ctx context.Context, pi peer.AddrInfo) error {
			mu.Lock()
			started++
			inFlight++
			mu.Unlock()
			<-release
			mu.Lock()
			inFlight--
			mu.Unlock()
			return nil
		},
	}
	frt.h = stub

	var err error
	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(_ context.Context, _ peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_FIND_NODE, req.Type)
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			// Each target is reported with its own unreachable address so every call
			// takes the "reported but not connected" path and starts a background dial.
			for i, tgt := range targets {
				addr, err := ma.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", 1+i))
				require.NoError(t, err)
				resp.CloserPeers = append(resp.CloserPeers, dht_pb.RawPeerInfosToPBPeers([]peer.AddrInfo{{ID: tgt, Addrs: []ma.Multiaddr{addr}}})...)
			}
			return resp, nil
		},
	})
	require.NoError(t, err)

	var maxInFlight int32
	stopObserving := make(chan struct{})
	defer close(stopObserving)
	go func() {
		for {
			select {
			case <-stopObserving:
				return
			case <-time.After(5 * time.Millisecond):
				mu.Lock()
				if inFlight > maxInFlight {
					maxInFlight = inFlight
				}
				mu.Unlock()
			}
		}
	}()

	for _, target := range targets {
		pi, err := frt.FindPeer(t.Context(), target)
		require.NoError(t, err)
		require.Equal(t, target, pi.ID)
	}

	mu.Lock()
	defer mu.Unlock()
	// Every Connect blocks until the test ends and the FindPeer calls are sequential,
	// so exactly the bound number of dials started: the excess were skipped, not
	// queued (a blocking acquire would deadlock the loop above) and not leaked past
	// the bound.
	require.Equal(t, int32(defaultMaxConcurrentFindPeerDials), started, "dials beyond the bound must be skipped, not started")
	require.LessOrEqual(t, maxInFlight, int32(defaultMaxConcurrentFindPeerDials), "more than the bound of background dials were in flight at once")
}

// TestFindPeerGraceCancelRace hammers the grace window under -race: many responders
// report the target concurrently while a tiny grace fires cancelquery. The only
// channel between the query goroutines and FindPeer's collector is addrsCh, which is
// closed after execOnMany returns; this test exists to catch a late send racing that
// close (a panic) or any other data race in the cancel path.
//
// It also pins the drain-after-cancel: responders[0] answers well before the grace
// deadline with its own address, so its report is buffered in addrsCh before the timer
// fires. The collector may not have consumed it by the time cancelquery() runs; the
// drain loop must still collect it after the cancel. A report whose round-trip only
// completes *after* the deadline is a different case and is intentionally dropped (see
// TestFindPeerGraceDropsPostDeadlineReports).
func TestFindPeerGraceCancelRace(t *testing.T) {
	const iters = 30

	for i := range iters {
		frt := newTestFullRT(t, WithTimeoutPerOperation(5*time.Second), WithFindPeerGrace(200*time.Millisecond))
		frt.bucketSize = 8
		responders := make([]peer.ID, 8)
		for j := range responders {
			responders[j] = newTestResponder(t)
		}
		setTriePeers(t, frt, responders...)

		target := newTestResponder(t)
		baseAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1")
		require.NoError(t, err)
		earlyAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/2")
		require.NoError(t, err)

		frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
			sendRequest: func(ctx context.Context, p peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
				assert.Equal(t, dht_pb.Message_FIND_NODE, req.Type)
				idx := 0
				for k, r := range responders {
					if r == p {
						idx = k
						break
					}
				}
				// Every responder reports the target. responders[0] answers early with
				// its own address: it opens the grace window and its report is buffered
				// before the deadline, so it must survive the cancel. The rest answer at
				// a delay that can land on either side of the grace window; whichever
				// side they land on, their baseAddr report is already covered by
				// responders[0].
				var addr ma.Multiaddr = baseAddr
				if idx == 0 {
					select {
					case <-time.After(20 * time.Millisecond):
						addr = earlyAddr
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				} else {
					delay := time.Duration((i+idx)%7) * 50 * time.Millisecond
					select {
					case <-time.After(delay):
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				resp := dht_pb.NewMessage(req.Type, req.Key, 0)
				resp.CloserPeers = dht_pb.RawPeerInfosToPBPeers([]peer.AddrInfo{{ID: target, Addrs: []ma.Multiaddr{addr}}})
				return resp, nil
			},
		})
		require.NoError(t, err)

		pi, err := frt.FindPeer(t.Context(), target)
		require.NoError(t, err)
		require.Equal(t, target, pi.ID)
		got := addrStrings(pi.Addrs)
		require.Contains(t, got, baseAddr.String())
		require.Contains(t, got, earlyAddr.String(), "a report buffered before the grace deadline must survive the cancel")
	}
}

// TestFindPeerGraceDropsPostDeadlineReports pins the other side of the grace window:
// a responder that answers after the deadline is cut off. Its send blocks on the full
// channel until cancelquery() unblocks it, then takes ctx.Done() and abandons the
// report, so its unique address must NOT be in the result. The len(peers) buffer makes
// this deterministic: with a smaller buffer the blocked sender could win the race to
// the collector instead of losing to the cancel.
func TestFindPeerGraceDropsPostDeadlineReports(t *testing.T) {
	const grace = 200 * time.Millisecond

	frt := newTestFullRT(t, WithTimeoutPerOperation(5*time.Second), WithFindPeerGrace(grace))
	frt.bucketSize = 8

	responders := make([]peer.ID, 8)
	for j := range responders {
		responders[j] = newTestResponder(t)
	}
	setTriePeers(t, frt, responders...)

	target := newTestResponder(t)
	baseAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1")
	require.NoError(t, err)
	slowAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/2")
	require.NoError(t, err)

	var firstReported atomic.Bool
	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(ctx context.Context, p peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_FIND_NODE, req.Type)
			idx := 0
			for k, r := range responders {
				if r == p {
					idx = k
					break
				}
			}
			var addr ma.Multiaddr = baseAddr
			switch idx {
			case 0:
				// The first responder answers immediately, opening the grace window.
				firstReported.Store(true)
			case 1:
				// The second responder waits for the first report, then sleeps well past
				// the grace deadline. By the time it tries to send, cancelquery() has
				// already fired and its ctx is done, so the send takes ctx.Done() and
				// the report is dropped.
				for !firstReported.Load() {
					select {
					case <-time.After(1 * time.Millisecond):
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				select {
				case <-time.After(grace + 50*time.Millisecond):
					addr = slowAddr
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			default:
				delay := time.Duration(idx) * 10 * time.Millisecond
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			resp.CloserPeers = dht_pb.RawPeerInfosToPBPeers([]peer.AddrInfo{{ID: target, Addrs: []ma.Multiaddr{addr}}})
			return resp, nil
		},
	})
	require.NoError(t, err)

	pi, err := frt.FindPeer(t.Context(), target)
	require.NoError(t, err)
	require.Equal(t, target, pi.ID)
	got := addrStrings(pi.Addrs)
	require.Contains(t, got, baseAddr.String())
	require.NotContains(t, got, slowAddr.String(), "a report that lands after the grace deadline must be dropped")
}

// TestFindPeerDialOutlivesCallerContext pins that the background dial is parented to
// the DHT's own context rather than the request context: cancelling the caller's
// context after FindPeer returns must not cancel the in-flight dial. A regression back
// to context.WithTimeout(ctx, ...) would make Connect return immediately with a
// cancellation error and this test would fail.
func TestFindPeerDialOutlivesCallerContext(t *testing.T) {
	frt := newTestFullRT(t, WithTimeoutPerOperation(5*time.Second), WithFindPeerGrace(0))
	frt.bucketSize = 3
	setTriePeers(t, frt, newTestResponder(t), newTestResponder(t), newTestResponder(t))

	target := newTestResponder(t)
	targetAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/1")
	require.NoError(t, err)
	reported := []peer.AddrInfo{{ID: target, Addrs: []ma.Multiaddr{targetAddr}}}

	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(_ context.Context, _ peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_FIND_NODE, req.Type)
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			resp.CloserPeers = dht_pb.RawPeerInfosToPBPeers(reported)
			return resp, nil
		},
	})
	require.NoError(t, err)

	dialStarted := make(chan struct{})
	var dialRanToCompletion atomic.Bool
	stub := &recordingHost{
		Host: frt.h,
		connect: func(ctx context.Context, pi peer.AddrInfo) error {
			close(dialStarted)
			// Hold the dial well past the caller's cancellation below. If the dial's
			// context were derived from the request context, it would be done by now and
			// this select would take the ctx.Done() branch.
			select {
			case <-time.After(200 * time.Millisecond):
				dialRanToCompletion.Store(true)
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	frt.h = stub

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pi, err := frt.FindPeer(ctx, target)
	require.NoError(t, err)
	require.Equal(t, target, pi.ID)

	// The dial must have started before FindPeer returned.
	select {
	case <-dialStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("the background dial did not start")
	}

	// Cancel the caller's context while the dial is still in flight.
	cancel()

	require.Eventually(t, func() bool { return dialRanToCompletion.Load() }, 2*time.Second, 10*time.Millisecond,
		"the background dial must run to completion after the caller's context is cancelled")
}

// TestFindPeerSelfReturnsNotFound pins that FindPeer for the DHT's own ID reports
// not-found rather than success with an empty AddrInfo: maybeAddAddrs declines to store
// addresses for self, so without the guard the final PeerInfo lookup would return an
// empty answer.
func TestFindPeerSelfReturnsNotFound(t *testing.T) {
	frt := newTestFullRT(t, WithTimeoutPerOperation(5*time.Second), WithFindPeerGrace(0))
	frt.bucketSize = 3
	setTriePeers(t, frt, newTestResponder(t), newTestResponder(t), newTestResponder(t))

	selfAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/5")
	require.NoError(t, err)

	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(_ context.Context, _ peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_FIND_NODE, req.Type)
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			resp.CloserPeers = dht_pb.RawPeerInfosToPBPeers([]peer.AddrInfo{{ID: frt.h.ID(), Addrs: []ma.Multiaddr{selfAddr}}})
			return resp, nil
		},
	})
	require.NoError(t, err)

	_, err = frt.FindPeer(t.Context(), frt.h.ID())
	require.ErrorIs(t, err, routing.ErrNotFound)
}

// TestFindPeerCallerCancelledSurfacesError pins that a caller context cancelled
// mid-query is surfaced as an error rather than presented as a successful lookup with
// whatever partial addresses happened to be collected, and that no background dial
// starts for a result nobody will read.
func TestFindPeerCallerCancelledSurfacesError(t *testing.T) {
	frt := newTestFullRT(t, WithTimeoutPerOperation(5*time.Second), WithFindPeerGrace(0))
	frt.bucketSize = 3
	setTriePeers(t, frt, newTestResponder(t), newTestResponder(t), newTestResponder(t))

	target := newTestResponder(t)
	targetAddr, err := ma.NewMultiaddr("/ip4/127.0.0.1/tcp/6")
	require.NoError(t, err)
	reported := []peer.AddrInfo{{ID: target, Addrs: []ma.Multiaddr{targetAddr}}}

	responders := make([]peer.ID, 3)
	for i := range responders {
		responders[i] = newTestResponder(t)
	}
	setTriePeers(t, frt, responders...)

	var connectCalls atomic.Int32
	stub := &recordingHost{
		Host: frt.h,
		connect: func(context.Context, peer.AddrInfo) error {
			connectCalls.Add(1)
			return nil
		},
	}
	frt.h = stub

	// responders[0] reports the target immediately; the rest block until the query
	// context is cancelled. That way the caller's cancellation lands after a report
	// has been collected (partial addresses would be available) but while the query
	// is still running.
	frt.protoMessenger, err = dht_pb.NewProtocolMessenger(&testMessageSender{
		sendRequest: func(ctx context.Context, p peer.ID, req *dht_pb.Message) (*dht_pb.Message, error) {
			assert.Equal(t, dht_pb.Message_FIND_NODE, req.Type)
			if p != responders[0] {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			resp := dht_pb.NewMessage(req.Type, req.Key, 0)
			resp.CloserPeers = dht_pb.RawPeerInfosToPBPeers(reported)
			return resp, nil
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		// Let the query report the target (so partial addresses would be available),
		// then cancel the caller's context before FindPeer returns.
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	_, err = frt.FindPeer(ctx, target)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, connectCalls.Load(), "no background dial may start for a cancelled caller")
}

// recordingHost wraps a host and overrides Connect so a test can record dial
// attempts. All other methods are passed through to the wrapped host, including
// Peerstore: the background dials must see the real peerstore, since FindPeer
// records the reported addresses there before dialing.
type recordingHost struct {
	host.Host
	connect func(ctx context.Context, pi peer.AddrInfo) error
}

var _ host.Host = (*recordingHost)(nil)

func (h *recordingHost) Connect(ctx context.Context, pi peer.AddrInfo) error {
	return h.connect(ctx, pi)
}

// reportingCrawler reports a fixed set of peers through handleSuccess and then
// returns, without dialling any of them. That is the shape of a crawler that
// replays a persisted routing table, and the case the default route table
// filter rejects: the host has no connection to any peer it reports.
type reportingCrawler struct {
	peers []peer.ID
	ran   chan struct{}
}

var _ crawler.Crawler = (*reportingCrawler)(nil)

func newReportingCrawler(peers ...peer.ID) *reportingCrawler {
	return &reportingCrawler{peers: peers, ran: make(chan struct{})}
}

func (c *reportingCrawler) Run(_ context.Context, _ []*peer.AddrInfo, handleSuccess crawler.HandleQueryResult, _ crawler.HandleQueryFail) {
	for _, p := range c.peers {
		handleSuccess(p, nil)
	}
	close(c.ran)
}

// waitForCrawl blocks until runCrawler has finished rebuilding the routing table
// from a crawl. Without it a test that asserts the table is *empty* would pass
// before the crawl had run at all, which is no assertion.
func waitForCrawl(t *testing.T, frt *FullRT) {
	t.Helper()
	require.Eventually(t, func() bool {
		frt.rtLk.RLock()
		defer frt.rtLk.RUnlock()
		return !frt.lastCrawlTime.IsZero()
	}, 10*time.Second, 5*time.Millisecond, "the crawl never completed")
}

// TestRouteTableFilterDefaultDropsUnreportedPeers is the regression guard for the
// whole change: with no option the default kaddht.PublicRoutingTableFilter still
// applies, and a peer the crawler reports without an open connection to it is
// kept out of the routing table.
func TestRouteTableFilterDefaultDropsUnconnectedPeers(t *testing.T) {
	peers := []peer.ID{newTestResponder(t), newTestResponder(t), newTestResponder(t)}
	c := newReportingCrawler(peers...)

	frt := newTestFullRT(t, WithCrawler(c))
	<-c.ran
	waitForCrawl(t, frt)

	require.Empty(t, frt.Stat(), "the default filter must drop peers the host has no connection to")
	require.False(t, frt.Ready(), "an empty routing table is not ready")
	for _, p := range peers {
		require.Empty(t, frt.h.Network().ConnsToPeer(p), "the stub crawler must not have dialled anything")
	}
}

// TestRouteTableFilterSuppliedFilterIsHonoured is the point of the option: a
// caller whose crawler reports peers it did not dial can supply a filter that
// admits them, and they reach the routing table.
func TestRouteTableFilterSuppliedFilterIsHonoured(t *testing.T) {
	peers := []peer.ID{newTestResponder(t), newTestResponder(t), newTestResponder(t)}
	c := newReportingCrawler(peers...)

	var seen []peer.ID
	var gotDHT []any
	var seenLk sync.Mutex
	frt := newTestFullRT(t, WithCrawler(c), WithRouteTableFilter(func(d any, p peer.ID) bool {
		seenLk.Lock()
		defer seenLk.Unlock()
		seen = append(seen, p)
		gotDHT = append(gotDHT, d)
		return true
	}))
	<-c.ran
	waitForCrawl(t, frt)

	stat := frt.Stat()
	require.Len(t, stat, len(peers))
	inTable := make(map[peer.ID]struct{}, len(stat))
	for _, p := range stat {
		inTable[p] = struct{}{}
	}
	for _, p := range peers {
		require.Contains(t, inTable, p)
	}

	seenLk.Lock()
	require.ElementsMatch(t, peers, seen, "the filter must see every peer the crawler reports")
	// The filter receives the *FullRT itself, which is what lets a real caller
	// delegate to kaddht.PublicRoutingTableFilter and widen it rather than
	// reimplement it.
	for _, d := range gotDHT {
		require.Same(t, frt, d)
	}
	seenLk.Unlock()

	// bootstrapPeers is empty here, so Ready needs a table larger than 1.
	require.Empty(t, frt.bootstrapPeers)
	require.True(t, frt.Ready(), "a freshly crawled table above the bootstrap count is ready")
}

// TestRouteTableFilterRejectAll pins the other end of the range.
func TestRouteTableFilterRejectAll(t *testing.T) {
	c := newReportingCrawler(newTestResponder(t), newTestResponder(t), newTestResponder(t))

	frt := newTestFullRT(t, WithCrawler(c), WithRouteTableFilter(func(any, peer.ID) bool { return false }))
	<-c.ran
	waitForCrawl(t, frt)

	require.Empty(t, frt.Stat())
	require.False(t, frt.Ready())
}

// TestWithRouteTableFilterRejectsNil keeps the option from installing a filter
// that would panic on the first crawl.
func TestWithRouteTableFilterRejectsNil(t *testing.T) {
	h, err := libp2p.New(libp2p.NoListenAddrs)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, h.Close()) })

	_, err = NewFullRT(h, "", WithCrawler(blockingCrawler{}), DHTOption(kaddht.BootstrapPeers()), WithRouteTableFilter(nil))
	require.ErrorContains(t, err, "route table filter must not be nil")
}
