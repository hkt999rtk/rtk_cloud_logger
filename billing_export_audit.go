package cloudlogger

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"time"

	"github.com/hkt999rtk/rtk_cloud_logger/billingarchive"
	bolt "go.etcd.io/bbolt"
)

// ValidateBillingSnapshotExports binds every authenticated NDJSON export to the
// matching snapshot's original record bytes and Logger-trusted receipt clock.
// It must succeed before issuing a completion or any subset range proof.
func ValidateBillingSnapshotExports(ctx context.Context, path string, m billingarchive.Manifest, exports io.Reader) error {
	db, err := bolt.Open(path, 0600, &bolt.Options{ReadOnly: true, Timeout: 100 * time.Millisecond})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.View(func(tx *bolt.Tx) error {
		meta := tx.Bucket([]byte("meta"))
		events := tx.Bucket([]byte("events"))
		if meta == nil || events == nil || string(meta.Get([]byte("identity"))) != m.StoreID || events.Sequence() != m.HighWater {
			return ErrBillingUnavailable
		}
		from := readUint(meta.Get([]byte("captured_through"))) + 1
		floor := readUint(meta.Get([]byte("archive_floor")))
		if from <= floor {
			from = floor + 1
		}
		if from > m.HighWater {
			from = 0
		}
		if from != m.FromSequence {
			return ErrBillingUnavailable
		}
		limit := int64(0)
		for _, p := range m.Parts {
			if p.Kind == "events" {
				limit += p.PlainBytes
			}
		}
		scan := bufio.NewScanner(io.LimitReader(exports, limit+1))
		scan.Buffer(make([]byte, 64<<10), 2<<20)
		var count uint64
		maxReceived := time.Time{}
		bindings := sha256.New()
		_, _ = bindings.Write([]byte("["))
		for scan.Scan() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var e billingarchive.ExportRecord
			if billingarchive.StrictDecode(bytes.NewReader(scan.Bytes()), 2<<20, &e) != nil || count >= m.RecordCount || e.Sequence != m.FromSequence+count {
				return ErrBillingUnavailable
			}
			stored := events.Get(sequenceKey(e.Sequence))
			r, err := ValidateBillingExportRecord(e)
			if err != nil || !bytes.Equal(stored, e.Record) || !sourceEnvironmentAllowed(r.Event.Env, m.SourceEventEnvironments) {
				return ErrBillingUnavailable
			}
			at, err := receiptTime(tx, e.Sequence)
			if err != nil || !at.Equal(e.ReceivedAt) {
				return ErrBillingUnavailable
			}
			var bind receiptBinding
			if json.Unmarshal(tx.Bucket([]byte("ids")).Get([]byte(r.Event.EventID)), &bind) != nil || bind.Sequence != e.Sequence || bind.ContentSHA256 != e.ContentSHA256 || bind.RecordSHA256 != billingarchive.Digest(e.Record) {
				return ErrBillingUnavailable
			}
			usageRaw, err := json.Marshal(r.Event.Fields["usage_event"])
			if err != nil {
				return err
			}
			ref, err := billingarchive.CanonicalUsageBinding(e.Sequence, e.ContentSHA256, usageRaw)
			if err != nil {
				return err
			}
			raw, _ := json.Marshal(ref)
			if count > 0 {
				_, _ = bindings.Write([]byte(","))
			}
			_, _ = bindings.Write(raw)
			if at.After(maxReceived) {
				maxReceived = at
			}
			count++
		}
		if err := scan.Err(); err != nil {
			return err
		}
		_, _ = bindings.Write([]byte("]"))
		if count != m.RecordCount || !maxReceived.Equal(m.MaxReceivedAt) || hex.EncodeToString(bindings.Sum(nil)) != m.RecordsBindingsSHA256 {
			return ErrBillingUnavailable
		}
		return nil
	})
}
