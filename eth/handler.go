// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/forkid"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/downloader"
	"github.com/ethereum/go-ethereum/eth/fetcher"
	"github.com/ethereum/go-ethereum/eth/reedsolomon"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/willf/bitset"
	"math"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	softResponseLimit = 2 * 1024 * 1024 // Target maximum size of returned blocks, headers or node data.
	estHeaderRlpSize  = 500             // Approximate size of an RLP encoded block header

	// txChanSize is the size of channel listening to NewTxsEvent.
	// The number is referenced from the size of tx pool.
	txChanSize = 4096

	//fragChanSize is the size of channel listening to fragMsg
	// The number is referenced from the size of tx pool
	fragsChanSize = 4096

	// minimum number of peers to broadcast new blocks to
	minBroadcastPeers = 4

	// minimum number of frags to try to decode
	minFragNum = 40

	// maximum number of total frags to send request
	maxTotalFrag = 80

	// request will not be sent to upper node when count result of bitmap exceeds the number
	upperRequestNum = 5

	// maximum number of decoded Fragments to store
	maxDecodeNum = 1024

	// number or Fragments each peer to send
	PeerFragsNum = 8

	// time intervall to force request.
	forceRequestCycle = 5 * time.Second

	// delay threshold
	// multiply 100 only for test !
	delayThreshold = 100 * time.Millisecond
)

var (
	syncChallengeTimeout = 15 * time.Second // Time allowance for a node to reply to the sync progress challenge
)

type fragMsg struct {
	frags *reedsolomon.Fragments
	code  uint64
	from  *peer
	td    *big.Int
}

func errResp(code errCode, format string, v ...interface{}) error {
	return fmt.Errorf("%v - %v", code, fmt.Sprintf(format, v...))
}

type decodedFrags struct {
	mutex sync.Mutex
	queue []reedsolomon.FragHash
}

func newDecodedFrags() *decodedFrags {
	return &decodedFrags{
		mutex: sync.Mutex{},
		queue: make([]reedsolomon.FragHash, 0),
	}
}

type ProtocolManager struct {
	networkID  uint64
	forkFilter forkid.Filter // Fork ID filter, constant across the lifetime of the node

	fastSync  uint32 // Flag whether fast sync is enabled (gets disabled if we already have blocks)
	acceptTxs uint32 // Flag whether we're considered synchronised (enables transaction processing)

	checkpointNumber uint64      // Block number for the sync progress validator to cross reference
	checkpointHash   common.Hash // Block hash for the sync progress validator to cross reference

	txpool     txPool
	fragpool   *reedsolomon.FragPool
	blockchain *core.BlockChain
	rs         *reedsolomon.RSCodec
	maxPeers   int
	decoded    *decodedFrags

	downloader *downloader.Downloader
	fetcher    *fetcher.Fetcher
	peers      *peerSet

	eventMux      *event.TypeMux
	txsCh         chan core.NewTxsEvent
	txsSub        event.Subscription
	minedBlockSub *event.TypeMuxSubscription
	fragsCh       chan fragMsg

	whitelist map[uint64]common.Hash

	// channels for fetcher, syncer, txsyncLoop
	newPeerCh          chan *peer
	txsyncCh           chan *txsync
	quitSync           chan struct{}
	quitFragsBroadcast chan struct{}
	quitInspector      chan struct{}
	noMorePeers        chan struct{}

	// wait group is used for graceful shutdowns during downloading
	// and processing
	wg sync.WaitGroup
}

// NewProtocolManager returns a new Ethereum sub protocol manager. The Ethereum sub protocol manages peers capable
// with the Ethereum network.
func NewProtocolManager(config *params.ChainConfig, checkpoint *params.TrustedCheckpoint, mode downloader.SyncMode, networkID uint64,
	mux *event.TypeMux, rs *reedsolomon.RSCodec, fragpool *reedsolomon.FragPool, txpool txPool, engine consensus.Engine,
	blockchain *core.BlockChain, chaindb ethdb.Database, cacheLimit int, whitelist map[uint64]common.Hash) (*ProtocolManager, error) {
	// Create the protocol manager with the base fields
	manager := &ProtocolManager{
		networkID:          networkID,
		forkFilter:         forkid.NewFilter(blockchain),
		eventMux:           mux,
		rs:                 rs,
		txpool:             txpool,
		fragpool:           fragpool,
		blockchain:         blockchain,
		peers:              newPeerSet(),
		decoded:            newDecodedFrags(),
		whitelist:          whitelist,
		newPeerCh:          make(chan *peer),
		noMorePeers:        make(chan struct{}),
		txsyncCh:           make(chan *txsync),
		quitSync:           make(chan struct{}),
		quitFragsBroadcast: make(chan struct{}),
		quitInspector:      make(chan struct{}),
	}
	if mode == downloader.FullSync {
		// The database seems empty as the current block is the genesis. Yet the fast
		// block is ahead, so fast sync was enabled for this node at a certain point.
		// The scenarios where this can happen is
		// * if the user manually (or via a bad block) rolled back a fast sync node
		//   below the sync point.
		// * the last fast sync is not finished while user specifies a full sync this
		//   time. But we don't have any recent state for full sync.
		// In these cases however it's safe to reenable fast sync.
		fullBlock, fastBlock := blockchain.CurrentBlock(), blockchain.CurrentFastBlock()
		if fullBlock.NumberU64() == 0 && fastBlock.NumberU64() > 0 {
			manager.fastSync = uint32(1)
			log.Warn("Switch sync mode from full sync to fast sync")
		}
	} else {
		if blockchain.CurrentBlock().NumberU64() > 0 {
			// Print warning log if database is not empty to run fast sync.
			log.Warn("Switch sync mode from fast sync to full sync")
		} else {
			// If fast sync was requested and our database is empty, grant it
			manager.fastSync = uint32(1)
		}
	}
	// If we have trusted checkpoints, enforce them on the chain
	if checkpoint != nil {
		manager.checkpointNumber = (checkpoint.SectionIndex+1)*params.CHTFrequency - 1
		manager.checkpointHash = checkpoint.SectionHead
	}

	// Construct the downloader (long sync) and its backing state bloom if fast
	// sync is requested. The downloader is responsible for deallocating the state
	// bloom when it's done.
	var stateBloom *trie.SyncBloom
	if atomic.LoadUint32(&manager.fastSync) == 1 {
		stateBloom = trie.NewSyncBloom(uint64(cacheLimit), chaindb)
	}
	manager.downloader = downloader.New(manager.checkpointNumber, chaindb, stateBloom, manager.eventMux, blockchain, nil, manager.removePeer)

	// Construct the fetcher (short sync)
	validator := func(header *types.Header) error {
		return engine.VerifyHeader(blockchain, header, true)
	}
	heighter := func() uint64 {
		return blockchain.CurrentBlock().NumberU64()
	}
	inserter := func(blocks types.Blocks) (int, error) {
		// If sync hasn't reached the checkpoint yet, deny importing weird blocks.
		//
		// Ideally we would also compare the head block's timestamp and similarly reject
		// the propagated block if the head is too old. Unfortunately there is a corner
		// case when starting new networks, where the genesis might be ancient (0 unix)
		// which would prevent full nodes from accepting it.
		if manager.blockchain.CurrentBlock().NumberU64() < manager.checkpointNumber {
			log.Warn("Unsynced yet, discarded propagated block", "number", blocks[0].Number(), "hash", blocks[0].Hash())
			return 0, nil
		}
		// If fast sync is running, deny importing weird blocks. This is a problematic
		// clause when starting up a new network, because fast-syncing miners might not
		// accept each others' blocks until a restart. Unfortunately we haven't figured
		// out a way yet where nodes can decide unilaterally whether the network is new
		// or not. This should be fixed if we figure out a solution.
		if atomic.LoadUint32(&manager.fastSync) == 1 {
			log.Warn("Fast syncing, discarded propagated block", "number", blocks[0].Number(), "hash", blocks[0].Hash())
			return 0, nil
		}
		n, err := manager.blockchain.InsertChain(blocks)
		if err == nil {
			atomic.StoreUint32(&manager.acceptTxs, 1) // Mark initial sync done on any fetcher import
		}
		return n, err
	}
	manager.fetcher = fetcher.New(blockchain.GetBlockByHash, validator, manager.BroadcastBlock, heighter, inserter, manager.removePeer)

	return manager, nil
}

func (pm *ProtocolManager) makeProtocol(version uint) p2p.Protocol {
	length, ok := protocolLengths[version]
	if !ok {
		panic("makeProtocol for unknown version")
	}

	return p2p.Protocol{
		Name:    protocolName,
		Version: version,
		Length:  length,
		Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
			peer := pm.newPeer(int(version), p, rw)
			select {
			case pm.newPeerCh <- peer:
				pm.wg.Add(1)
				defer pm.wg.Done()
				return pm.handle(peer)
			case <-pm.quitSync:
				return p2p.DiscQuitting
			}
		},
		NodeInfo: func() interface{} {
			return pm.NodeInfo()
		},
		PeerInfo: func(id enode.ID) interface{} {
			if p := pm.peers.Peer(fmt.Sprintf("%x", id[:8])); p != nil {
				return p.Info()
			}
			return nil
		},
	}
}

func (pm *ProtocolManager) removePeer(id string) {
	// Short circuit if the peer was already removed
	peer := pm.peers.Peer(id)
	if peer == nil {
		return
	}
	log.Debug("Removing Ethereum peer", "peer", id)

	// Unregister the peer from the downloader and Ethereum peer set
	pm.downloader.UnregisterPeer(id)
	if err := pm.peers.Unregister(id); err != nil {
		log.Error("Peer removal failed", "peer", id, "err", err)
	}
	// Hard disconnect at the networking layer
	if peer != nil {
		peer.Peer.Disconnect(p2p.DiscUselessPeer)
	}
}

// trigger a fragment requestFrags
func (pm *ProtocolManager) requestFrags(idx reedsolomon.FragHash, fragType uint64, minHopPeer string) {
	var p *peer
	var ok bool

	// satisfy minHop first
	if p, ok = pm.peers.SearchPeer(minHopPeer); !ok {
		if p, ok = pm.peers.RandomPeer(); !ok {
			log.Warn("no peers, cannot request")
			return
		}
	}
	
	pm.fragpool.BigMutex.Lock()
	bit := pm.fragpool.Load[idx].Bit
	pm.fragpool.BigMutex.Unlock()
	req := reedsolomon.Request{
		Load: bit,
		ID:   idx,
	}
	p.SendRequest(idx, bit, fragType)
	log.Trace("Send Frags Request","ID", idx, "Type", fragType,"reqsize", req.Size())
}

func (pm *ProtocolManager) requestFragsByBitmap(idx reedsolomon.FragHash, fragType uint64, minHopPeer string, bit *bitset.BitSet) {
	var p *peer
	var ok bool

	// satisfy minHop first
	if p, ok = pm.peers.SearchPeer(minHopPeer); !ok {
		if p, ok = pm.peers.RandomPeer(); !ok {
			log.Warn("no peers, cannot request")
			return
		}
	}
	req := reedsolomon.Request{
		Load: bit,
		ID:   idx,
	}
	p.SendRequest(idx, bit, fragType)
	log.Trace("Send Frags Request(recursive)","ID", idx, "Type", fragType,"reqsize", req.Size())
}

// inspect over whether need to requestFrags
func (pm *ProtocolManager) inspector() {
	var temp map[reedsolomon.FragHash]uint64
	temp = make(map[reedsolomon.FragHash]uint64, 0)

	forceRequest := time.NewTicker(forceRequestCycle)
	defer forceRequest.Stop()

	for {
		select {
		case <-forceRequest.C:
			pm.fragpool.BigMutex.Lock()
			for k, v := range pm.fragpool.Load {
				if _, flag := temp[k]; !flag {
					temp[k] = v.Cnt
				} else {
					if temp[k] != v.Cnt {
						temp[k] = v.Cnt
					} else if v.IsDecoded == 0 {
						go pm.requestFrags(k,v.Type,v.MinHopPeer)
					}
				}
			}
			pm.fragpool.BigMutex.Unlock()
		case <-pm.quitInspector:
			return
		}
	}
}

func (pm *ProtocolManager) Start(maxPeers int) {
	pm.maxPeers = maxPeers

	// broadcast transactions
	pm.txsCh = make(chan core.NewTxsEvent, txChanSize)
	pm.txsSub = pm.txpool.SubscribeLocalTxsEvent(pm.txsCh)
	go pm.txBroadcastLoop()

	// broadcast mined blocks
	pm.minedBlockSub = pm.eventMux.Subscribe(core.NewMinedBlockEvent{})
	go pm.minedBroadcastLoop()
	// go pm.inspector()

	// broadcast fragments
	pm.fragsCh = make(chan fragMsg, fragsChanSize)
	go pm.fragsBroadcastLoop()
	// start sync handlers
	go pm.syncer()
	go pm.txsyncLoop()
}

func (pm *ProtocolManager) Stop() {
	log.Info("Stopping Ethereum protocol")

	pm.txsSub.Unsubscribe()        // quits txBroadcastLoop
	pm.minedBlockSub.Unsubscribe() // quits blockBroadcastLoop

	// Quit the sync loop.
	// After this send has completed, no new peers will be accepted.
	pm.noMorePeers <- struct{}{}

	// Quit fetcher, txsyncLoop.
	close(pm.quitSync)
	close(pm.quitInspector)
	close(pm.quitFragsBroadcast)

	// Disconnect existing sessions.
	// This also closes the gate for any new registrations on the peer set.
	// sessions which are already established but not added to pm.peers yet
	// will exit when they try to register.
	pm.peers.Close()

	// Wait for all peer handler goroutines and the loops to come down.
	pm.wg.Wait()

	log.Info("Ethereum protocol stopped")
}

func (pm *ProtocolManager) newPeer(pv int, p *p2p.Peer, rw p2p.MsgReadWriter) *peer {
	return newPeer(pv, p, newMeteredMsgWriter(rw))
}

// handle is the callback invoked to manage the life cycle of an eth peer. When
// this function terminates, the peer is disconnected.
func (pm *ProtocolManager) handle(p *peer) error {
	// Ignore maxPeers if this is a trusted peer
	if pm.peers.Len() >= pm.maxPeers && !p.Peer.Info().Network.Trusted {
		return p2p.DiscTooManyPeers
	}
	p.Log().Debug("Ethereum peer connected", "name", p.Name())

	// Execute the Ethereum handshake
	var (
		genesis = pm.blockchain.Genesis()
		head    = pm.blockchain.CurrentHeader()
		hash    = head.Hash()
		number  = head.Number.Uint64()
		td      = pm.blockchain.GetTd(hash, number)
	)
	if err := p.Handshake(pm.networkID, td, hash, genesis.Hash(), forkid.NewID(pm.blockchain), pm.forkFilter); err != nil {
		p.Log().Debug("Ethereum handshake failed", "err", err)
		return err
	}
	if rw, ok := p.rw.(*meteredMsgReadWriter); ok {
		rw.Init(p.version)
	}
	// Register the peer locally
	if err := pm.peers.Register(p); err != nil {
		p.Log().Error("Ethereum peer registration failed", "err", err)
		return err
	}
	defer pm.removePeer(p.id)

	// Register the peer in the downloader. If the downloader considers it banned, we disconnect
	if err := pm.downloader.RegisterPeer(p.id, p.version, p); err != nil {
		return err
	}
	// Propagate existing transactions. new transactions appearing
	// after this will be sent via broadcasts.
	pm.syncTransactions(p)

	// If we have a trusted CHT, reject all peers below that (avoid fast sync eclipse)
	if pm.checkpointHash != (common.Hash{}) {
		// Request the peer's checkpoint header for chain height/weight validation
		if err := p.RequestHeadersByNumber(pm.checkpointNumber, 1, 0, false); err != nil {
			return err
		}
		// Start a timer to disconnect if the peer doesn't reply in time
		p.syncDrop = time.AfterFunc(syncChallengeTimeout, func() {
			p.Log().Warn("Checkpoint challenge timed out, dropping", "addr", p.RemoteAddr(), "type", p.Name())
			pm.removePeer(p.id)
		})
		// Make sure it's cleaned up if the peer dies off
		defer func() {
			if p.syncDrop != nil {
				p.syncDrop.Stop()
				p.syncDrop = nil
			}
		}()
	}
	// If we have any explicit whitelist block hashes, requestFrags them
	for number := range pm.whitelist {
		if err := p.RequestHeadersByNumber(number, 1, 0, false); err != nil {
			return err
		}
	}
	// Handle incoming messages until the connection is torn down
	for {
		if err := pm.handleMsg(p); err != nil {
			p.Log().Debug("Ethereum message handling failed", "err", err)
			return err
		}
	}
}

// handleMsg is invoked whenever an inbound message is received from a remote
// peer. The remote connection is torn down upon returning any error.
func (pm *ProtocolManager) handleMsg(p *peer) error {
	// Read the next message from the remote peer, and ensure it's fully consumed
	msg, err := p.rw.ReadMsg()
	//fmt.Printf("lzr:msg received, code: %d,from %x\n\n", msg.Code, p.id)
	if err != nil {
		return err
	}
	if msg.Size > protocolMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, protocolMaxMsgSize)
	}
	defer msg.Discard()

	// Handle the message depending on its contents
	switch {
	case msg.Code == StatusMsg:
		// Status messages should never arrive after the handshake
		return errResp(ErrExtraStatusMsg, "uncontrolled status message")

	case msg.Code == TxFragMsg:
		// Frags arrived, make sure we have a valid and fresh chain to handle them
		if atomic.LoadUint32(&pm.acceptTxs) == 0 {
			break
		}
		// Transaction fragments can be processed, parse all of them and deliver to the pool
		var cnt uint64
		var totalFrag uint64
		var isDecoded uint32
		var frags reedsolomon.Fragments
		if err := msg.Decode(&frags); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		//if pm.txpool.CheckExistence(frags.ID) != nil {
		//	break
		//}
		//p.MarkTransaction(frags.ID)
		fragPos := make([]uint8, 0)
		for _, frag := range frags.Frags {
			// Validate and mark the remote transaction
			cnt, totalFrag, isDecoded = pm.fragpool.Insert(frag, frags.ID, frags.HopCnt, p.id, nil, msg.Code)
			fragPos = append(fragPos, frag.Pos())
		}
		log.Trace("Receive Fragments","ID", frags.ID, "Cnt", cnt, "TotalFrag", totalFrag, "Pos", fragPos)

		frags.HopCnt++
		log.Trace("TxFrags HopCnt ++", "ID",frags.ID, "HopCnt",frags.HopCnt, "peerID", p.id)
		select {
		case pm.fragsCh <- fragMsg{
			frags: &frags,
			code:  msg.Code,
			from:  p,
			td:    nil,
		}:
		default:
		}
		if cnt >= minFragNum && isDecoded == 0 {
			txRlp, flag := pm.fragpool.TryDecode(frags.ID, pm.rs)
			// flag=1 means decode success
			if flag {
				var tx types.Transaction
				err = rlp.Decode(bytes.NewReader(txRlp), &tx)
				if err != nil {
					return err
				}
				if &tx == nil {
					return errResp(ErrDecode, "transaction is nil")
				}
				//if tx.Hash() != frags.ID {
				//	return errResp(ErrDecode, "RS decode is wrong")
				//}

				txs := make([]*types.Transaction, 0)
				txs = append(txs, &tx)
				errs := pm.txpool.AddRemotes(txs) // do not need
				for _, err = range errs {
					if err != nil {
						log.Error("Error in TxFragMsg", "error:", err)
					}
				}

				// Clean maybe unneeded trash
				pm.decoded.mutex.Lock()
				pm.decoded.queue = append(pm.decoded.queue, frags.ID)
				for l := len(pm.decoded.queue); l > maxDecodeNum; l-- {
					id := pm.decoded.queue[0]
					pm.fragpool.Clean(id)
					pm.decoded.queue = pm.decoded.queue[1:]
				}
				pm.decoded.mutex.Unlock()
			} else {
				panic("RS cannot decode")
			}
		} else if totalFrag >= maxTotalFrag && isDecoded == 0{
			log.Trace("Try to request","ID", frags.ID, "Cnt", cnt, "TotalFrag", totalFrag)

			pm.fragpool.BigMutex.Lock()
			line, flag := pm.fragpool.Load[frags.ID]
			pm.fragpool.BigMutex.Unlock()
			if !flag {
				fmt.Printf("\nOops! Tx Fragments have been dropped!\n")
				break
			}

			oldReqing := line.SetIsReqing()
			if oldReqing == 0 {
				log.Trace("Request was already sent.", "ID", frags.ID)
				go pm.requestFrags(frags.ID, TxFragMsg, line.MinHopPeer)
			}
		}

		// a response to a former request
		if frags.IsResp == 1 {
			log.Trace("Receive Tx Response","ID", frags.ID)
			pm.fragpool.BigMutex.Lock()
			line, _ := pm.fragpool.Load[frags.ID]

			// clear waiting list
			oldHead := line.ClearReq()
			pm.fragpool.BigMutex.Unlock()

			for node := oldHead; node!= nil; node = node.Next {
				respFrags := pm.fragpool.Prepare(&reedsolomon.Request{
					Load: node.Bit,
					ID:   frags.ID,
				})


				np, ok := pm.peers.SearchPeer(node.PeerID)
				if !ok{
					log.Warn("Cannot find exact peer!")
					continue
				}
				log.Trace("Response to RequestTxFragMsg(recursive)","ID", respFrags.ID,"frag size",respFrags.Size(), "PeerID", node.PeerID)
				np.SendTxFragments(respFrags)
			}
		}

	case msg.Code == BlockFragMsg:
		var cnt uint64
		var isDecoded uint32
		var totalFrag uint64
		var reqfrag newBlockFragData
		if err := msg.Decode(&reqfrag); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		frags := reqfrag.Frags

		fragPos := make([]uint8, 0)
		for _, frag := range frags.Frags {
			cnt, totalFrag, isDecoded = pm.fragpool.Insert(frag, frags.ID, frags.HopCnt, p.id, reqfrag.TD, msg.Code)
			fragPos = append(fragPos, frag.Pos())
		}
		log.Trace("Receive Fragments","ID", frags.ID, "Cnt", cnt, "TotalFrag", totalFrag, "Pos", fragPos)

		frags.HopCnt++
		log.Trace("BlockFrags HopCnt ++", "ID",frags.ID, "HopCnt",frags.HopCnt, "peerID", p.id)
		select {
		case pm.fragsCh <- fragMsg{
			frags: frags,
			code:  msg.Code,
			from:  p,
			td:    reqfrag.TD,
		}:
		default:
		}
		if cnt >= minFragNum && isDecoded == 0 {
			blockrlp, flag := pm.fragpool.TryDecode(frags.ID, pm.rs)
			if flag {
				var block types.Block
				if err = rlp.Decode(bytes.NewReader(blockrlp), &block); err != nil {
					return errResp(ErrDecode, "%v: %v", msg, err)
				}
				log.Trace("Block RSdecode successful", "ID", block.Hash(), "peerID", p.id)
				//if block.Hash() != frags.ID {
				//	return errResp(ErrDecode, "wrong RS decode result")
				//}
				var request newBlockData
				request.Block = &block
				if reqfrag.TD != nil {
					request.TD = reqfrag.TD
				} else {
					// Frags come from former Request
					pm.fragpool.BigMutex.Lock()
					request.TD = pm.fragpool.Load[frags.ID].TD
					pm.fragpool.BigMutex.Unlock()
				}
				if err = request.sanityCheck(); err != nil {
					return err
				}
				request.Block.ReceivedAt = msg.ReceivedAt
				request.Block.ReceivedFrom = p

				// Mark the peer as owning the block and schedule it for import
				pm.fetcher.Enqueue(p.id, request.Block)

				// Assuming the block is importable by the peer, but possibly not yet done so,
				// calculate the head hash and TD that the peer truly must have.
				var (
					trueHead = request.Block.ParentHash()
					trueTD   = new(big.Int).Sub(request.TD, request.Block.Difficulty())
				)
				// Update the peer's total difficulty if better than the previous
				if _, td := p.Head(); trueTD.Cmp(td) > 0 {
					p.SetHead(trueHead, trueTD)

					// Schedule a sync if above ours. Note, this will not fire a sync for a gap of
					// a single block (as the true TD is below the propagated block), however this
					// scenario should easily be covered by the fetcher.
					currentBlock := pm.blockchain.CurrentBlock()
					if trueTD.Cmp(pm.blockchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64())) > 0 {
						go pm.synchronise(p)
					}
				}
				// Clean maybe unneeded trash
				pm.decoded.mutex.Lock()
				pm.decoded.queue = append(pm.decoded.queue, frags.ID)
				for l := len(pm.decoded.queue); l > maxDecodeNum; l-- {
					id := pm.decoded.queue[0]
					pm.fragpool.Clean(id)
					pm.decoded.queue = pm.decoded.queue[1:]
				}
				pm.decoded.mutex.Unlock()
			} else {
				log.Debug("cannot RS decode")
			}
		} else if totalFrag >= maxTotalFrag && isDecoded == 0 {
			log.Trace("Try to request","ID", frags.ID, "Cnt", cnt, "TotalFrag", totalFrag)

			pm.fragpool.BigMutex.Lock()
			line, flag := pm.fragpool.Load[frags.ID]
			pm.fragpool.BigMutex.Unlock()
			if !flag {
				fmt.Printf("\nOops! Block Fragments have been dropped!\n")
				break
			}
			
			oldReqing := line.SetIsReqing()
			if oldReqing == 0 {
				log.Trace("Request was already sent.", "ID", frags.ID)
				go pm.requestFrags(frags.ID, BlockFragMsg, line.MinHopPeer)
			}
		}

		// a response to a former request
		if frags.IsResp == 1 {
			log.Trace("Receive Block Response","ID", frags.ID)
			pm.fragpool.BigMutex.Lock()
			line, _ := pm.fragpool.Load[frags.ID]

			// clear waiting list
			oldHead := line.ClearReq()
			pm.fragpool.BigMutex.Unlock()

			for node := oldHead; node!= nil; node = node.Next {
				respFrags := pm.fragpool.Prepare(&reedsolomon.Request{
					Load: node.Bit,
					ID:   frags.ID,
				})


				np, ok := pm.peers.SearchPeer(node.PeerID)
				if !ok{
					log.Warn("Cannot find exact peer!")
					continue
				}
				log.Trace("Response to RequestBlockFragMsg(recursive)","ID", respFrags.ID,"frag size",respFrags.Size(), "PeerID", node.PeerID)
				np.SendBlockFragments(respFrags, nil)
			}
		}

	case msg.Code == RequestTxFragMsg:
		// Transaction fragments can be processed, parse all of them and deliver to the pool
		var frags *reedsolomon.Fragments
		var req newRequestFragData
		if err := msg.Decode(&req); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// already decode successfully
		pm.fragpool.BigMutex.Lock()
		line, flag := pm.fragpool.Load[req.ID]
		pm.fragpool.BigMutex.Unlock()
		if !flag {
			fmt.Printf("\nOops! Tx Fragments have been dropped!\n")
			break
		}

		// deliver request to upper node
		bit := bitset.From(req.Set)
		merge_bit := bit.Union(line.Bit)
		if merge_bit.Count() < upperRequestNum {

			// insert it as a reponse-waiting request
			log.Trace("Insert unresp tx req","ID", req.ID, "PeerID", p.id)
			oldReqing := line.InsertReq(bit, p.id)
			if oldReqing == 0 {
				go pm.requestFragsByBitmap(req.ID, TxFragMsg, line.MinHopPeer, merge_bit)
			}
			break
		}

		// return fragments immediately
		frags = pm.fragpool.Prepare(&reedsolomon.Request{
			Load: bit,
			ID:   req.ID,
		})
		log.Trace("Response to RequestTxFragMsg","ID", frags.ID, "fragsize",frags.Size(), "PeerID", p.id,)
		return p.SendTxFragments(frags)
		//p2p.Send(p.rw, TxFragMsg, frags)

	case msg.Code == RequestBlockFragMsg:
		var frags *reedsolomon.Fragments
		var req newRequestFragData
		if err := msg.Decode(&req); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// already decode successfully
		pm.fragpool.BigMutex.Lock()
		line, flag := pm.fragpool.Load[req.ID]
		pm.fragpool.BigMutex.Unlock()
		if !flag {
			log.Trace("\nOops! Block Fragments have been dropped!\n")
			break
		}

		// deliver request to upper node
		bit := bitset.From(req.Set)
		merge_bit := bit.Union(line.Bit)
		if merge_bit.Count() < upperRequestNum {

			// insert it as a reponse-waiting request
			log.Trace("Insert unresp block req","ID", req.ID, "PeerID", p.id)
			oldReqing := line.InsertReq(bit, p.id)
			if oldReqing == 0 {
				go pm.requestFragsByBitmap(req.ID, BlockFragMsg, line.MinHopPeer, merge_bit)
			}
			break
		}

		// return fragments immediately
		frags = pm.fragpool.Prepare(&reedsolomon.Request{
			Load: bit,
			ID:   req.ID,
		})
		log.Trace("Response to RequestBlockFragMsg", "ID", frags.ID, "fragsize", frags.Size(), "PeerID", p.id)
		return p.SendBlockFragments(frags, nil)
		//p2p.Send(p.rw, BlockFragMsg, frags)

	// Block header query, collect the requested headers and reply
	case msg.Code == GetBlockHeadersMsg:
		// Decode the complex header query
		var query getBlockHeadersData
		if err := msg.Decode(&query); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		hashMode := query.Origin.Hash != (common.Hash{})
		first := true
		maxNonCanonical := uint64(100)

		// Gather headers until the fetch or network limits is reached
		var (
			bytes   common.StorageSize
			headers []*types.Header
			unknown bool
		)
		for !unknown && len(headers) < int(query.Amount) && bytes < softResponseLimit && len(headers) < downloader.MaxHeaderFetch {
			// Retrieve the next header satisfying the query
			var origin *types.Header
			if hashMode {
				if first {
					first = false
					origin = pm.blockchain.GetHeaderByHash(query.Origin.Hash)
					if origin != nil {
						query.Origin.Number = origin.Number.Uint64()
					}
				} else {
					origin = pm.blockchain.GetHeader(query.Origin.Hash, query.Origin.Number)
				}
			} else {
				origin = pm.blockchain.GetHeaderByNumber(query.Origin.Number)
			}
			if origin == nil {
				break
			}
			headers = append(headers, origin)
			bytes += estHeaderRlpSize

			// Advance to the next header of the query
			switch {
			case hashMode && query.Reverse:
				// Hash based traversal towards the genesis block
				ancestor := query.Skip + 1
				if ancestor == 0 {
					unknown = true
				} else {
					query.Origin.Hash, query.Origin.Number = pm.blockchain.GetAncestor(query.Origin.Hash, query.Origin.Number, ancestor, &maxNonCanonical)
					unknown = (query.Origin.Hash == common.Hash{})
				}
			case hashMode && !query.Reverse:
				// Hash based traversal towards the leaf block
				var (
					current = origin.Number.Uint64()
					next    = current + query.Skip + 1
				)
				if next <= current {
					infos, _ := json.MarshalIndent(p.Peer.Info(), "", "  ")
					p.Log().Warn("GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next, "attacker", infos)
					unknown = true
				} else {
					if header := pm.blockchain.GetHeaderByNumber(next); header != nil {
						nextHash := header.Hash()
						expOldHash, _ := pm.blockchain.GetAncestor(nextHash, next, query.Skip+1, &maxNonCanonical)
						if expOldHash == query.Origin.Hash {
							query.Origin.Hash, query.Origin.Number = nextHash, next
						} else {
							unknown = true
						}
					} else {
						unknown = true
					}
				}
			case query.Reverse:
				// Number based traversal towards the genesis block
				if query.Origin.Number >= query.Skip+1 {
					query.Origin.Number -= query.Skip + 1
				} else {
					unknown = true
				}

			case !query.Reverse:
				// Number based traversal towards the leaf block
				query.Origin.Number += query.Skip + 1
			}
		}
		return p.SendBlockHeaders(headers)

	case msg.Code == BlockHeadersMsg:
		// A batch of headers arrived to one of our previous requests
		var headers []*types.Header
		if err := msg.Decode(&headers); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// If no headers were received, but we're expencting a checkpoint header, consider it that
		if len(headers) == 0 && p.syncDrop != nil {
			// Stop the timer either way, decide later to drop or not
			p.syncDrop.Stop()
			p.syncDrop = nil

			// If we're doing a fast sync, we must enforce the checkpoint block to avoid
			// eclipse attacks. Unsynced nodes are welcome to connect after we're done
			// joining the network
			if atomic.LoadUint32(&pm.fastSync) == 1 {
				p.Log().Warn("Dropping unsynced node during fast sync", "addr", p.RemoteAddr(), "type", p.Name())
				return errors.New("unsynced node cannot serve fast sync")
			}
		}
		// Filter out any explicitly requested headers, deliver the rest to the downloader
		filter := len(headers) == 1
		if filter {
			// If it's a potential sync progress check, validate the content and advertised chain weight
			if p.syncDrop != nil && headers[0].Number.Uint64() == pm.checkpointNumber {
				// Disable the sync drop timer
				p.syncDrop.Stop()
				p.syncDrop = nil

				// Validate the header and either drop the peer or continue
				if headers[0].Hash() != pm.checkpointHash {
					return errors.New("checkpoint hash mismatch")
				}
				return nil
			}
			// Otherwise if it's a whitelisted block, validate against the set
			if want, ok := pm.whitelist[headers[0].Number.Uint64()]; ok {
				if hash := headers[0].Hash(); want != hash {
					p.Log().Info("Whitelist mismatch, dropping peer", "number", headers[0].Number.Uint64(), "hash", hash, "want", want)
					return errors.New("whitelist block mismatch")
				}
				p.Log().Debug("Whitelist block verified", "number", headers[0].Number.Uint64(), "hash", want)
			}
			// Irrelevant of the fork checks, send the header to the fetcher just in case
			headers = pm.fetcher.FilterHeaders(p.id, headers, time.Now())
		}
		if len(headers) > 0 || !filter {
			err := pm.downloader.DeliverHeaders(p.id, headers)
			if err != nil {
				log.Debug("Failed to deliver headers", "err", err)
			}
		}

	case msg.Code == GetBlockBodiesMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather blocks until the fetch or network limits is reached
		var (
			hash   common.Hash
			bytes  int
			bodies []rlp.RawValue
		)
		for bytes < softResponseLimit && len(bodies) < downloader.MaxBlockFetch {
			// Retrieve the hash of the next block
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested block body, stopping if enough was found
			if data := pm.blockchain.GetBodyRLP(hash); len(data) != 0 {
				bodies = append(bodies, data)
				bytes += len(data)
			}
		}
		return p.SendBlockBodiesRLP(bodies)

	case msg.Code == BlockBodiesMsg:
		// A batch of block bodies arrived to one of our previous requests
		var request blockBodiesData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver them all to the downloader for queuing
		transactions := make([][]*types.Transaction, len(request))
		uncles := make([][]*types.Header, len(request))

		for i, body := range request {
			transactions[i] = body.Transactions
			uncles[i] = body.Uncles
		}
		// Filter out any explicitly requested bodies, deliver the rest to the downloader
		filter := len(transactions) > 0 || len(uncles) > 0
		if filter {
			transactions, uncles = pm.fetcher.FilterBodies(p.id, transactions, uncles, time.Now())
		}
		if len(transactions) > 0 || len(uncles) > 0 || !filter {
			err := pm.downloader.DeliverBodies(p.id, transactions, uncles)
			if err != nil {
				log.Debug("Failed to deliver bodies", "err", err)
			}
		}

	case p.version >= eth63 && msg.Code == GetNodeDataMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather state data until the fetch or network limits is reached
		var (
			hash  common.Hash
			bytes int
			data  [][]byte
		)
		for bytes < softResponseLimit && len(data) < downloader.MaxStateFetch {
			// Retrieve the hash of the next state entry
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested state entry, stopping if enough was found
			if entry, err := pm.blockchain.TrieNode(hash); err == nil {
				data = append(data, entry)
				bytes += len(entry)
			}
		}
		return p.SendNodeData(data)

	case p.version >= eth63 && msg.Code == NodeDataMsg:
		// A batch of node state data arrived to one of our previous requests
		var data [][]byte
		if err := msg.Decode(&data); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver all to the downloader
		if err := pm.downloader.DeliverNodeData(p.id, data); err != nil {
			log.Debug("Failed to deliver node state data", "err", err)
		}

	case p.version >= eth63 && msg.Code == GetReceiptsMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather state data until the fetch or network limits is reached
		var (
			hash     common.Hash
			bytes    int
			receipts []rlp.RawValue
		)
		for bytes < softResponseLimit && len(receipts) < downloader.MaxReceiptFetch {
			// Retrieve the hash of the next block
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested block's receipts, skipping if unknown to us
			results := pm.blockchain.GetReceiptsByHash(hash)
			if results == nil {
				if header := pm.blockchain.GetHeaderByHash(hash); header == nil || header.ReceiptHash != types.EmptyRootHash {
					continue
				}
			}
			// If known, encode and queue for response packet
			if encoded, err := rlp.EncodeToBytes(results); err != nil {
				log.Error("Failed to encode receipt", "err", err)
			} else {
				receipts = append(receipts, encoded)
				bytes += len(encoded)
			}
		}
		return p.SendReceiptsRLP(receipts)

	case p.version >= eth63 && msg.Code == ReceiptsMsg:
		// A batch of receipts arrived to one of our previous requests
		var receipts [][]*types.Receipt
		if err := msg.Decode(&receipts); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver all to the downloader
		if err := pm.downloader.DeliverReceipts(p.id, receipts); err != nil {
			log.Debug("Failed to deliver receipts", "err", err)
		}

	case msg.Code == NewBlockHashesMsg:
		var announces newBlockHashesData
		if err := msg.Decode(&announces); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		// Mark the hashes as present at the remote node
		for _, block := range announces {
			p.MarkBlock(block.Hash)
		}
		// Schedule all the unknown hashes for retrieval
		unknown := make(newBlockHashesData, 0, len(announces))
		for _, block := range announces {
			if !pm.blockchain.HasBlock(block.Hash, block.Number) {
				unknown = append(unknown, block)
			}
		}
		for _, block := range unknown {
			pm.fetcher.Notify(p.id, block.Hash, block.Number, time.Now(), p.RequestOneHeader, p.RequestBodies)
		}

	case msg.Code == NewBlockMsg:
		// Retrieve and decode the propagated block
		var request newBlockData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		if err := request.sanityCheck(); err != nil {
			return err
		}
		request.Block.ReceivedAt = msg.ReceivedAt
		request.Block.ReceivedFrom = p

		// Mark the peer as owning the block and schedule it for import
		p.MarkBlock(request.Block.Hash())
		pm.fetcher.Enqueue(p.id, request.Block)

		// Assuming the block is importable by the peer, but possibly not yet done so,
		// calculate the head hash and TD that the peer truly must have.
		var (
			trueHead = request.Block.ParentHash()
			trueTD   = new(big.Int).Sub(request.TD, request.Block.Difficulty())
		)
		// Update the peer's total difficulty if better than the previous
		if _, td := p.Head(); trueTD.Cmp(td) > 0 {
			p.SetHead(trueHead, trueTD)

			// Schedule a sync if above ours. Note, this will not fire a sync for a gap of
			// a single block (as the true TD is below the propagated block), however this
			// scenario should easily be covered by the fetcher.
			currentBlock := pm.blockchain.CurrentBlock()
			if trueTD.Cmp(pm.blockchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64())) > 0 {
				go pm.synchronise(p)
			}
		}

	case msg.Code == TxMsg:
		// Transactions arrived, make sure we have a valid and fresh chain to handle them
		if atomic.LoadUint32(&pm.acceptTxs) == 0 {
			break
		}
		// Transactions can be processed, parse all of them and deliver to the pool
		var txs []*types.Transaction
		if err := msg.Decode(&txs); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		for i, tx := range txs {
			// Validate and mark the remote transaction
			if tx == nil {
				return errResp(ErrDecode, "transaction %d is nil", i)
			}
			p.MarkTransaction(tx.Hash())
		}
		pm.txpool.AddRemotes(txs)

	default:
		return errResp(ErrInvalidMsgCode, "%v", msg.Code)
	}
	return nil
}

// BroadcastBlock will either propagate a block to a subset of it's peers, or
// will only announce it's availability (depending what's requested).
func (pm *ProtocolManager) BroadcastBlock(block *types.Block, propagate bool) {
	hash := block.Hash()
	peers := pm.peers.PeersWithoutBlock(hash)

	// If propagation is requested, send to a subset of the peer
	if propagate {
		// Calculate the TD of the block (it's not imported yet, so block.Td is not valid)
		var td *big.Int
		if parent := pm.blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1); parent != nil {
			td = new(big.Int).Add(block.Difficulty(), pm.blockchain.GetTd(block.ParentHash(), block.NumberU64()-1))
		} else {
			log.Error("Propagating dangling block", "number", block.Number(), "hash", hash)
			return
		}
		// Send the block to a subset of our peers
		transferLen := int(math.Sqrt(float64(len(peers))))
		if transferLen < minBroadcastPeers {
			transferLen = minBroadcastPeers
		}
		if transferLen > len(peers) {
			transferLen = len(peers)
		}
		transfer := peers[:transferLen]
		for _, peer := range transfer {
			peer.AsyncSendNewBlock(block, td)
		}
		log.Trace("Propagated block", "hash", hash, "recipients", len(transfer), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
		return
	}
	// Otherwise if the block is indeed in out own chain, announce it
	if pm.blockchain.HasBlock(hash, block.NumberU64()) {
		for _, peer := range peers {
			peer.AsyncSendNewBlockHash(block)
		}
		log.Trace("Announced block", "hash", hash, "recipients", len(peers), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
	}
}

// BroadcastTxs will propagate a batch of transactions to all peers which are not known to
// already have the given transaction.
func (pm *ProtocolManager) BroadcastTxs(txs types.Transactions) {
	var txset = make(map[*peer]types.Transactions)

	// Broadcast transactions to a batch of peers not knowing about it
	for _, tx := range txs {
		peers := pm.peers.PeersWithoutTx(tx.Hash())
		for _, peer := range peers {
			txset[peer] = append(txset[peer], tx)
		}
		log.Trace("Broadcast transaction", "hash", tx.Hash(), "recipients", len(peers))
	}
	// FIXME include this again: peers = peers[:int(math.Sqrt(float64(len(peers))))]
	for peer, txs := range txset {
		peer.AsyncSendTransactions(txs)
	}
}

func (pm *ProtocolManager) BroadcastReceivedFrags(frags *reedsolomon.Fragments, msgCode uint64, from *peer, td *big.Int) {
	switch msgCode {
	case TxFragMsg:
		peers := pm.peers.PeersWithoutFragAndPeer(frags.ID, from)
		var wwg sync.WaitGroup
		peerNum := len(peers)
		wwg.Add(peerNum)
		for _, p := range peers {
			go func(p *peer, frags *reedsolomon.Fragments) {
				p.UpdateLatency()
				fmt.Println("forwardtxFrags-about to send: ", p.id, p.latency, time.Now().String())
				time.Sleep(p.latency)
				p.AsyncSendTxFrags(frags)
				fmt.Println("forwardFrags-send over: ", p.id, p.latency, time.Now().String())
				defer wwg.Done()
			}(p, frags)
		}
		wwg.Wait()
	case BlockFragMsg:
		peers := pm.peers.PeersWithoutFragAndPeer(frags.ID, from)
		var wwg sync.WaitGroup
		peerNum := len(peers)
		wwg.Add(peerNum)
		var fragindex0 []int
		for i, _ := range frags.Frags {
			fragindex0 = append(fragindex0, i)
		}
		peerFragsNum := PeerFragsNum
		if len(frags.Frags) < peerFragsNum {
			for _, p := range peers {
				go func(p *peer, frags *reedsolomon.Fragments, td *big.Int) {
					p.UpdateLatency()
					fmt.Println("forwardbkFrags-about to send: ", p.id, p.latency, time.Now().String())
					time.Sleep(p.latency)
					p.AsyncSendBlockFrags(frags, td)
					fmt.Println("forwardbkFrags-send over: ", p.id, p.latency, time.Now().String())
					defer wwg.Done()
				}(p, frags, td)
			}
		} else {
			idx := 0
			fragindex := fragindex0
			for _, p := range peers {
				if peerFragsNum*(idx+1) > len(frags.Frags) {
					idx = 0
					rand.Shuffle(len(fragindex), func(i, j int) {
						fragindex[i], fragindex[j] = fragindex[j], fragindex[i]
					})
				}
				fragToSend := reedsolomon.NewFragments(0)
				for _, i := range fragindex[peerFragsNum*idx : peerFragsNum*(idx+1)] {
					fragToSend.Frags = append(fragToSend.Frags, frags.Frags[i])
				}
				fragToSend.ID = frags.ID
				go func(p *peer, frags *reedsolomon.Fragments, td *big.Int) {
					p.UpdateLatency()
					fmt.Println("sendbkFrags-about to send: ", p.id, p.latency, time.Now().String())
					time.Sleep(p.latency)
					p.AsyncSendBlockFrags(frags, td)
					fmt.Println("sendbkFrags-send over: ", p.id, p.latency, time.Now().String())
					defer wwg.Done()
				}(p, fragToSend, td)
				idx += 1
			}
		}
		wwg.Wait()
	}
}

func (pm *ProtocolManager) BroadcastTxFrags(frags *reedsolomon.Fragments) {
	//Broadcast transaction fragments to a batch of peers not knowing about it
	var fragindex0 []int
	for i, _ := range frags.Frags {
		fragindex0 = append(fragindex0, i)
	}
	peers := pm.peers.PeersWithoutFrag(frags.ID)
	peerFragsNum := PeerFragsNum

	var wwg sync.WaitGroup
	peerNum := len(peers)
	wwg.Add(peerNum)

	if len(frags.Frags) < peerFragsNum {
		for _, p := range peers {
			go func(p *peer, frags *reedsolomon.Fragments) {
				p.UpdateLatency()
				fmt.Println("sendtxFrags-about to send: ", p.id, p.latency, time.Now().String())
				time.Sleep(p.latency)
				p.AsyncSendTxFrags(frags)
				fmt.Println("sendtxFrags-send over: ", p.id, p.latency, time.Now().String())
				defer wwg.Done()
			}(p, frags)
		}
	} else {
		idx := 0
		fragindex := fragindex0
		for _, p := range peers {
			if peerFragsNum*(idx+1) > len(frags.Frags) {
				idx = 0
				rand.Shuffle(len(fragindex), func(i, j int) {
					fragindex[i], fragindex[j] = fragindex[j], fragindex[i]
				})
			}
			fragToSend := reedsolomon.NewFragments(0)
			for _, i := range fragindex[peerFragsNum*idx : peerFragsNum*(idx+1)] {
				fragToSend.Frags = append(fragToSend.Frags, frags.Frags[i])
			}
			fragToSend.ID = frags.ID
			go func(p *peer, frags *reedsolomon.Fragments) {
				p.UpdateLatency()
				fmt.Println("sendtxFrags-about to send: ", p.id, p.latency, time.Now().String())
				time.Sleep(p.latency)
				p.AsyncSendTxFrags(frags)
				fmt.Println("sendtxFrags-send over: ", p.id, p.latency, time.Now().String())
				defer wwg.Done()
			}(p, fragToSend)
			idx += 1
		}
	}
	wwg.Wait()
	//log.Trace("Broadcalist Tx fragments : ", "Tx hash", frags.ID, "recipients", len(peers))
}

func (pm *ProtocolManager) BroadcastBlockFrags(frags *reedsolomon.Fragments, td *big.Int) {
	//Broadcast block fragments to a batch of peers not knowing about it

	peers := pm.peers.PeersWithoutFrag(frags.ID)

	list1 := make([]*peer, 0, len(pm.peers.peers))
	list2 := make([]*peer, 0, len(pm.peers.peers))

	for _, peer := range peers {
		peer.UpdateLatency()
		fmt.Println("zirui:", peer.id, peer.latency)
		if peer.latency < delayThreshold {
			list1 = append(list1, peer)
		} else {
			list2 = append(list2, peer)
		}
	}

	//fmt.Println("peers:", peers)
	//fmt.Println("list1:", list1)
	//fmt.Println("list2:", list2)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		fmt.Println("***list1 send", list1, time.Now().String(), frags.ID)
		pm.BroadcastMyBlockFrags(list1, frags, td)
		defer wg.Done()
	}()
	//time.Sleep(time.Millisecond * 100)
	go func() {
		fmt.Println("***list2 send", list2, time.Now().String(), frags.ID)
		pm.BroadcastMyBlockFrags(list2, frags, td)
		defer wg.Done()
	}()
	wg.Wait()
}

func (pm *ProtocolManager) BroadcastMyBlockFrags(peers []*peer, frags *reedsolomon.Fragments, td *big.Int) {
	var wwg sync.WaitGroup
	peerNum := len(peers)
	wwg.Add(peerNum)

	var fragindex0 []int
	for i, _ := range frags.Frags {
		fragindex0 = append(fragindex0, i)
	}
	peerFragsNum := minFragNum
	if len(frags.Frags) < peerFragsNum {
		//peerFragsNum = len(frags.Frags)
		for _, p := range peers {
			go func(p *peer, frags *reedsolomon.Fragments, td *big.Int) {
				p.UpdateLatency()
				fmt.Println("sendbkFrags-about to send: ", p.id, p.latency, time.Now().String())
				time.Sleep(p.latency)
				p.AsyncSendBlockFrags(frags, td)
				fmt.Println("sendbkFrags-send over: ", p.id, p.latency, time.Now().String())
				defer wwg.Done()
			}(p, frags, td)
		}
	} else {
		idx := 0
		fragindex := fragindex0
		for _, p := range peers {
			if peerFragsNum*(idx+1) > len(frags.Frags) {
				idx = 0
				rand.Shuffle(len(fragindex), func(i, j int) {
					fragindex[i], fragindex[j] = fragindex[j], fragindex[i]
				})
			}
			fragToSend := reedsolomon.NewFragments(0)
			for _, i := range fragindex[peerFragsNum*idx : peerFragsNum*(idx+1)] {
				fragToSend.Frags = append(fragToSend.Frags, frags.Frags[i])
			}
			fragToSend.ID = frags.ID
			go func(p *peer, frags *reedsolomon.Fragments, td *big.Int) {
				p.UpdateLatency()
				fmt.Println("sendbkFrags-about to send: ", p.id, p.latency, time.Now().String())
				time.Sleep(p.latency)
				p.AsyncSendBlockFrags(frags, td)
				fmt.Println("sendbkFrags-send over: ", p.id, p.latency, time.Now().String())
				defer wwg.Done()
			}(p, fragToSend, td)
			idx += 1
		}
	}
	wwg.Wait()
}

func (pm *ProtocolManager) BlockToFragments(block *types.Block) (*reedsolomon.Fragments, *big.Int) {
	var td *big.Int
	hash := block.Hash()
	if parent := pm.blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1); parent != nil {
		td = new(big.Int).Add(block.Difficulty(), pm.blockchain.GetTd(block.ParentHash(), block.NumberU64()-1))
	} else {
		log.Error("Propagating dangling block", "number", block.Number(), "hash", hash)
		return nil, nil
	}
	id := block.Hash()
	rlpCode, _ := rlp.EncodeToBytes(block)
	frags := pm.rs.DivideAndEncode(rlpCode)
	tmp := reedsolomon.NewFragments(0)
	var fingerPrint	reedsolomon.FragHash
	for i, item := range id {
		fingerPrint[i] = item
		if i == reedsolomon.HashLength - 1 {
			log.Debug("finger print of fragment", "fingerprint", fingerPrint)
			break
		}
	}
	tmp.ID = fingerPrint
	for _, frag := range frags {
		tmp.Frags = append(tmp.Frags, frag)
	}
	return tmp, td
}

func (pm *ProtocolManager) TxToFragments(tx *types.Transaction) *reedsolomon.Fragments {
	id := tx.Hash()
	rlpCode, _ := rlp.EncodeToBytes(tx)
	frags := pm.rs.DivideAndEncode(rlpCode)
	tmp := reedsolomon.NewFragments(0)
	var fingerPrint reedsolomon.FragHash
	for i, item := range id {
		fingerPrint[i] = item
		if i == reedsolomon.HashLength - 1 {
			break
		}
	}
	tmp.ID = fingerPrint
	for _, frag := range frags {
		tmp.Frags = append(tmp.Frags, frag)
	}
	return tmp
}

// Mined broadcast loop
func (pm *ProtocolManager) minedBroadcastLoop() {
	// automatically stops if unsubscribe
	for obj := range pm.minedBlockSub.Chan() {
		if ev, ok := obj.Data.(core.NewMinedBlockEvent); ok {
			frags, td := pm.BlockToFragments(ev.Block)
			if frags == nil {
				continue
			}
			for _, fragment := range frags.Frags {
 				pm.fragpool.Insert(fragment, frags.ID, frags.HopCnt, "", td, BlockFragMsg)
 			}
			pm.BroadcastBlockFrags(frags, td)
			//pm.BroadcastBlock(ev.Block, true) // First propagate block to peers
			//pm.BroadcastBlock(ev.Block, false) // Only then announce to the rest
		}
	}
}

func (pm *ProtocolManager) txBroadcastLoop() {
	for {
		select {
		case event := <-pm.txsCh:
			for _, tx := range event.Txs {
				frags := pm.TxToFragments(tx)
				if frags == nil {
 					continue
 				}
 				for _, fragment := range frags.Frags {
 					pm.fragpool.Insert(fragment, frags.ID, frags.HopCnt, "",nil, TxFragMsg)
 				}
				pm.BroadcastTxFrags(frags)
			}
			//pm.BroadcastTransactions(txs)
		// Err() channel will be closed when unsubscribing.
		case <-pm.txsSub.Err():
			return
		}
	}
}

func (pm *ProtocolManager) fragsBroadcastLoop() {
	for {
		select {
		case fragMsg := <-pm.fragsCh:
			pm.BroadcastReceivedFrags(fragMsg.frags, fragMsg.code, fragMsg.from, fragMsg.td)
		case <-pm.quitFragsBroadcast:
			return
		}
	}
}

// NodeInfo represents a short summary of the Ethereum sub-protocol metadata
// known about the host peer.
type NodeInfo struct {
	Network    uint64              `json:"network"`    // Ethereum network ID (1=Frontier, 2=Morden, Ropsten=3, Rinkeby=4)
	Difficulty *big.Int            `json:"difficulty"` // Total difficulty of the host's blockchain
	Genesis    common.Hash         `json:"genesis"`    // SHA3 hash of the host's genesis block
	Config     *params.ChainConfig `json:"config"`     // Chain configuration for the fork rules
	Head       common.Hash         `json:"head"`       // SHA3 hash of the host's best owned block
}

// NodeInfo retrieves some protocol metadata about the running host node.
func (pm *ProtocolManager) NodeInfo() *NodeInfo {
	currentBlock := pm.blockchain.CurrentBlock()
	return &NodeInfo{
		Network:    pm.networkID,
		Difficulty: pm.blockchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64()),
		Genesis:    pm.blockchain.Genesis().Hash(),
		Config:     pm.blockchain.Config(),
		Head:       currentBlock.Hash(),
	}
}
