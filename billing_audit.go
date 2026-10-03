package cloudlogger

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
	bolt "go.etcd.io/bbolt"
)

func validDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func sourceEnvironmentAllowed(env string, allowed []string) bool {
	for _, v := range allowed {
		if v == env {
			return true
		}
	}
	return false
}
func auditLifecycleTx(ctx context.Context, tx *bolt.Tx, environment string, sourceEnvironments []string, keys map[string]ed25519.PublicKey) error {
	meta := tx.Bucket([]byte("meta"))
	events := tx.Bucket([]byte("events"))
	if meta == nil || events == nil || string(meta.Get([]byte("version"))) != "2" {
		return ErrBillingUnavailable
	}
	allowed := map[string]bool{}
	for _, name := range []string{"meta", "events", "ids", "receipt_times", "receipt_ids", "catalog", "archive_cache", "operations", "mutation_journal", "captures"} {
		allowed[name] = true
		if tx.Bucket([]byte(name)) == nil {
			return ErrBillingUnavailable
		}
	}
	if err := tx.ForEach(func(k []byte, b *bolt.Bucket) error {
		if !allowed[string(k)] {
			return ErrBillingUnavailable
		}
		return nil
	}); err != nil {
		return err
	}
	var first error
	for err := range tx.Check() {
		if first == nil {
			first = err
		}
	}
	if first != nil {
		return ErrBillingUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	head := events.Sequence()
	for _, key := range []string{"mutation_generation", "legacy_through", "backfilled_through"} {
		if len(meta.Get([]byte(key))) != 8 {
			return ErrBillingUnavailable
		}
	}
	if readUint(meta.Get([]byte("mutation_generation"))) == 0 {
		return ErrBillingUnavailable
	}
	for _, key := range []string{"archive_floor", "captured_through", "cache_bytes"} {
		if value := meta.Get([]byte(key)); value != nil && len(value) != 8 {
			return ErrBillingUnavailable
		}
	}
	floor := readUint(meta.Get([]byte("archive_floor")))
	legacy := readUint(meta.Get([]byte("legacy_through")))
	if floor > head || legacy > head || readUint(meta.Get([]byte("backfilled_through"))) != legacy {
		return ErrBillingUnavailable
	}
	if _, err := time.Parse(time.RFC3339Nano, string(meta.Get([]byte("legacy_age_floor")))); err != nil {
		return ErrBillingUnavailable
	}
	ids := tx.Bucket([]byte("ids"))
	receiptIDs := tx.Bucket([]byte("receipt_ids"))
	var count uint64
	if err := ids.ForEach(func(k, v []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var bind receiptBinding
		if len(k) == 0 || len(k) > 4096 || json.Unmarshal(v, &bind) != nil || bind.Sequence == 0 || bind.Sequence > head || !validDigest(bind.ContentSHA256) || !validDigest(bind.RecordSHA256) || !bytes.Equal(receiptIDs.Get(sequenceKey(bind.Sequence)), k) {
			return ErrBillingUnavailable
		}
		count++
		return nil
	}); err != nil {
		return err
	}
	if count != head {
		return ErrBillingUnavailable
	}
	for _, name := range []string{"receipt_times", "receipt_ids"} {
		var entries uint64
		if err := tx.Bucket([]byte(name)).ForEach(func(k, v []byte) error {
			seq := readUint(k)
			if len(k) != 8 || seq == 0 || seq > head || v == nil {
				return ErrBillingUnavailable
			}
			entries++
			return nil
		}); err != nil {
			return err
		}
		if entries != head {
			return ErrBillingUnavailable
		}
	}
	for seq := uint64(1); seq <= head; seq++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := receiptIDs.Get(sequenceKey(seq))
		if id == nil {
			return ErrBillingUnavailable
		}
		var bind receiptBinding
		if json.Unmarshal(ids.Get(id), &bind) != nil || bind.Sequence != seq {
			return ErrBillingUnavailable
		}
		if _, err := receiptTime(tx, seq); err != nil {
			return err
		}
		if seq > floor {
			r, err := decodeBillingRecord(events.Get(sequenceKey(seq)), seq)
			if err != nil || r.Event.EventID != string(id) || !sourceEnvironmentAllowed(r.Event.Env, sourceEnvironments) || bind.ContentSHA256 != r.ContentSHA256 || bind.RecordSHA256 != billingarchive.Digest(events.Get(sequenceKey(seq))) {
				return ErrBillingUnavailable
			}
		}
	}
	if err := events.ForEach(func(k, v []byte) error {
		seq := readUint(k)
		if len(k) != 8 || seq <= floor || seq > head || v == nil {
			return ErrBillingUnavailable
		}
		return nil
	}); err != nil {
		return err
	}
	type interval struct{ from, through uint64 }
	ranges := []interval{}
	catalog := tx.Bucket([]byte("catalog"))
	if err := catalog.ForEach(func(k, v []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var c CatalogEntry
		if json.Unmarshal(v, &c) != nil || c.Status != "verified" || c.Manifest.SetID != string(k) || c.Manifest.StoreID != string(meta.Get([]byte("identity"))) || c.Manifest.Environment != environment || c.Manifest.Validate() != nil {
			return ErrBillingUnavailable
		}
		parsed, err := billingarchive.DecodeManifest(bytes.NewReader(c.ManifestBytes))
		if err != nil || !equalJSON(parsed, c.Manifest) {
			return ErrBillingUnavailable
		}
		if keys != nil {
			if _, err := billingarchive.VerifyCompletion(c.Completion, c.Manifest, c.ManifestBytes, keys); err != nil {
				return ErrBillingUnavailable
			}
		} else {
			return ErrBillingUnavailable
		}
		if c.Manifest.RecordCount > 0 {
			ranges = append(ranges, interval{c.Manifest.FromSequence, c.Manifest.ThroughSequence})
		}
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].from < ranges[j].from })
	covered := uint64(0)
	for _, r := range ranges {
		if r.from > covered+1 {
			break
		}
		if r.through > covered {
			covered = r.through
		}
	}
	if floor > covered {
		return ErrBillingUnavailable
	}
	var cachedBytes uint64
	if err := tx.Bucket([]byte("archive_cache")).ForEach(func(k, v []byte) error {
		seq := readUint(k)
		if len(k) != 8 || seq == 0 || seq > floor {
			return ErrBillingUnavailable
		}
		r, err := decodeBillingRecord(v, seq)
		if err != nil {
			return err
		}
		var bind receiptBinding
		if json.Unmarshal(ids.Get([]byte(r.Event.EventID)), &bind) != nil || bind.Sequence != seq || bind.ContentSHA256 != r.ContentSHA256 || bind.RecordSHA256 != billingarchive.Digest(v) {
			return ErrBillingUnavailable
		}
		cachedBytes += uint64(len(v))
		return nil
	}); err != nil {
		return err
	}
	if cachedBytes != readUint(meta.Get([]byte("cache_bytes"))) {
		return ErrBillingUnavailable
	}
	completed := []interval{}
	if err := tx.Bucket([]byte("operations")).ForEach(func(k, v []byte) error {
		var st RetirementStatus
		if json.Unmarshal(v, &st) != nil || st.OperationID != string(k) || st.Environment != environment || st.StoreID != string(meta.Get([]byte("identity"))) || !validDigest(st.PlanSHA256) || st.FromSequence == 0 || st.ThroughSequence < st.FromSequence || st.ThroughSequence-st.FromSequence >= 1000 || st.ThroughSequence > head {
			return ErrBillingUnavailable
		}
		switch st.Status {
		case "pending":
			if st.ReceiptSHA256 != "" || !st.CompletedAt.IsZero() {
				return ErrBillingUnavailable
			}
		case "completed", "aborted":
			want := st
			sealStatus(&want)
			if want.ReceiptSHA256 != st.ReceiptSHA256 || st.CompletedAt.IsZero() || st.RetiredThrough > floor || (st.Status == "completed" && st.RetiredThrough < st.ThroughSequence) {
				return ErrBillingUnavailable
			}
			if st.Status == "completed" {
				if st.RetiredThrough != st.ThroughSequence {
					return ErrBillingUnavailable
				}
				completed = append(completed, interval{st.FromSequence, st.ThroughSequence})
			}
		default:
			return ErrBillingUnavailable
		}
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(completed, func(i, j int) bool { return completed[i].from < completed[j].from })
	decisionFloor := uint64(0)
	for _, r := range completed {
		if r.from != decisionFloor+1 {
			return ErrBillingUnavailable
		}
		decisionFloor = r.through
	}
	if decisionFloor != floor {
		return ErrBillingUnavailable
	}
	generation := readUint(meta.Get([]byte("mutation_generation")))
	return tx.Bucket([]byte("mutation_journal")).ForEach(func(k, v []byte) error {
		if len(k) != 8 || readUint(k) == 0 || readUint(k) > generation {
			return ErrBillingUnavailable
		}
		var writes []mutation
		if json.Unmarshal(v, &writes) != nil || len(writes) == 0 {
			return ErrBillingUnavailable
		}
		for _, w := range writes {
			if !allowed[w.Bucket] || w.Bucket == "mutation_journal" || (w.Kind != "put" && w.Kind != "delete" && w.Kind != "create" && w.Kind != "sequence") {
				return ErrBillingUnavailable
			}
		}
		return nil
	})
}
