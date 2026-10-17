package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/jamespfennell/subwaydata.nyc/etl/config"
	"github.com/jamespfennell/subwaydata.nyc/metadata"
)

const metadataPath = "metadata/nycsubway.json"

type fakeBucket struct {
	name    string
	failPut bool
	// log, if set, records the name of the bucket on every successful put.
	log *putLog

	mu      sync.Mutex
	objects map[string][]byte
}

type putLog struct {
	mu      sync.Mutex
	buckets []string
}

func (l *putLog) record(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buckets = append(l.buckets, name)
}

func newFakeBucket(name string) *fakeBucket {
	return &fakeBucket{name: name, objects: map[string][]byte{}}
}

func (b *fakeBucket) put(ctx context.Context, data []byte, remotePath string) error {
	if b.failPut {
		return fmt.Errorf("bucket %s: put failed", b.name)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.objects[remotePath] = append([]byte{}, data...)
	if b.log != nil {
		b.log.record(b.name)
	}
	return nil
}

func (b *fakeBucket) get(ctx context.Context, remotePath string) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[remotePath]
	if !ok {
		return nil, errNotFound
	}
	return data, nil
}

func (b *fakeBucket) object(remotePath string) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.objects[remotePath]
	return data, ok
}

func (b *fakeBucket) setMetadata(t *testing.T, m *metadata.Metadata) {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("failed to marshal metadata: %s", err)
	}
	b.objects[metadataPath] = data
}

func (b *fakeBucket) getMetadata(t *testing.T) *metadata.Metadata {
	t.Helper()
	data, ok := b.object(metadataPath)
	if !ok {
		t.Fatalf("bucket %s: no metadata written", b.name)
	}
	var m metadata.Metadata
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("bucket %s: failed to unmarshal metadata: %s", b.name, err)
	}
	return &m
}

func newTestClient() (*Client, *fakeBucket, *fakeBucket, *fakeBucket) {
	primary := newFakeBucket("primary")
	secondary1 := newFakeBucket("secondary1")
	secondary2 := newFakeBucket("secondary2")
	return newClient(metadataPath, primary, []bucket{secondary1, secondary2}), primary, secondary1, secondary2
}

func days(m *metadata.Metadata) []metadata.Day {
	var result []metadata.Day
	for _, processedDay := range m.ProcessedDays {
		result = append(result, processedDay.Day)
	}
	return result
}

var (
	jan1 = metadata.NewDay(2022, time.January, 1)
	jan2 = metadata.NewDay(2022, time.January, 2)
	jan3 = metadata.NewDay(2022, time.January, 3)
)

func TestWrite_WritesToAllBuckets(t *testing.T) {
	c, primary, secondary1, secondary2 := newTestClient()

	if err := c.Write(context.Background(), []byte("data"), "path/to/file"); err != nil {
		t.Fatalf("Write() failed: %s", err)
	}

	for _, b := range []*fakeBucket{primary, secondary1, secondary2} {
		data, ok := b.object("path/to/file")
		if !ok || string(data) != "data" {
			t.Errorf("bucket %s: got (%q, %t), want (%q, true)", b.name, data, ok, "data")
		}
	}
}

func TestWrite_FailsIfAnyBucketFails(t *testing.T) {
	for _, failing := range []string{"primary", "secondary1", "secondary2"} {
		t.Run(failing, func(t *testing.T) {
			c, primary, secondary1, secondary2 := newTestClient()
			for _, b := range []*fakeBucket{primary, secondary1, secondary2} {
				b.failPut = b.name == failing
			}

			if err := c.Write(context.Background(), []byte("data"), "path/to/file"); err == nil {
				t.Errorf("Write() succeeded, want error")
			}
		})
	}
}

func TestGetMetadata_ReadsFromPrimary(t *testing.T) {
	c, primary, secondary1, _ := newTestClient()
	primary.setMetadata(t, &metadata.Metadata{ProcessedDays: []metadata.ProcessedDay{{Day: jan1}}})
	secondary1.setMetadata(t, &metadata.Metadata{ProcessedDays: []metadata.ProcessedDay{{Day: jan2}}})

	m, err := c.GetMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetMetadata() failed: %s", err)
	}

	if got := days(m); len(got) != 1 || got[0] != jan1 {
		t.Errorf("GetMetadata() days = %v, want [%s]", got, jan1)
	}
}

func TestGetMetadata_MissingReturnsEmpty(t *testing.T) {
	c, _, _, _ := newTestClient()

	m, err := c.GetMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetMetadata() failed: %s", err)
	}

	if len(m.ProcessedDays) != 0 {
		t.Errorf("GetMetadata() days = %v, want none", days(m))
	}
}

func TestUpdateMetadata_WritesSortedMetadataToAllBuckets(t *testing.T) {
	c, primary, secondary1, secondary2 := newTestClient()
	primary.setMetadata(t, &metadata.Metadata{ProcessedDays: []metadata.ProcessedDay{{Day: jan2}}})

	err := c.UpdateMetadata(context.Background(), func(m *metadata.Metadata) bool {
		m.ProcessedDays = append(m.ProcessedDays, metadata.ProcessedDay{Day: jan1}, metadata.ProcessedDay{Day: jan3})
		return true
	})
	if err != nil {
		t.Fatalf("UpdateMetadata() failed: %s", err)
	}

	want := []metadata.Day{jan3, jan2, jan1}
	for _, b := range []*fakeBucket{primary, secondary1, secondary2} {
		if got := days(b.getMetadata(t)); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("bucket %s: metadata days = %v, want %v", b.name, got, want)
		}
	}
}

func TestUpdateMetadata_NoCommit(t *testing.T) {
	c, primary, secondary1, secondary2 := newTestClient()

	err := c.UpdateMetadata(context.Background(), func(m *metadata.Metadata) bool {
		m.ProcessedDays = append(m.ProcessedDays, metadata.ProcessedDay{Day: jan1})
		return false
	})
	if err != nil {
		t.Fatalf("UpdateMetadata() failed: %s", err)
	}

	for _, b := range []*fakeBucket{primary, secondary1, secondary2} {
		if _, ok := b.object(metadataPath); ok {
			t.Errorf("bucket %s: metadata written, want no write", b.name)
		}
	}
}

func TestUpdateMetadata_WritesPrimaryLast(t *testing.T) {
	c, primary, secondary1, secondary2 := newTestClient()
	log := &putLog{}
	for _, b := range []*fakeBucket{primary, secondary1, secondary2} {
		b.log = log
	}

	err := c.UpdateMetadata(context.Background(), func(m *metadata.Metadata) bool { return true })
	if err != nil {
		t.Fatalf("UpdateMetadata() failed: %s", err)
	}

	if len(log.buckets) != 3 || log.buckets[2] != "primary" {
		t.Errorf("put order = %v, want primary last", log.buckets)
	}
}

func TestUpdateMetadata_SecondaryFailureSkipsPrimary(t *testing.T) {
	c, primary, _, secondary2 := newTestClient()
	primary.setMetadata(t, &metadata.Metadata{ProcessedDays: []metadata.ProcessedDay{{Day: jan1}}})
	secondary2.failPut = true

	err := c.UpdateMetadata(context.Background(), func(m *metadata.Metadata) bool {
		m.ProcessedDays = append(m.ProcessedDays, metadata.ProcessedDay{Day: jan2})
		return true
	})
	if err == nil {
		t.Fatalf("UpdateMetadata() succeeded, want error")
	}

	if got := days(primary.getMetadata(t)); len(got) != 1 || got[0] != jan1 {
		t.Errorf("primary metadata days = %v, want [%s]", got, jan1)
	}
}

func TestUpdateMetadata_ConcurrentUpdatesAreNotLost(t *testing.T) {
	c, primary, _, _ := newTestClient()
	n := 20

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		day := jan1
		for j := 0; j < i; j++ {
			day = day.Next()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.UpdateMetadata(context.Background(), func(m *metadata.Metadata) bool {
				m.ProcessedDays = append(m.ProcessedDays, metadata.ProcessedDay{Day: day})
				return true
			})
			if err != nil {
				t.Errorf("UpdateMetadata() failed: %s", err)
			}
		}()
	}
	wg.Wait()

	if got := len(primary.getMetadata(t).ProcessedDays); got != n {
		t.Errorf("primary metadata has %d days, want %d", got, n)
	}
}

func TestS3Bucket_PutObjectInput(t *testing.T) {
	testCases := []struct {
		name    string
		acl     string
		wantAcl *string
	}{
		{
			name:    "with ACL",
			acl:     "public-read",
			wantAcl: aws.String("public-read"),
		},
		{
			name: "without ACL",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := newS3Bucket(config.Bucket{Url: "a.com", Name: "bucket", Prefix: "prefix", Acl: tc.acl})
			if err != nil {
				t.Fatalf("newS3Bucket() failed: %s", err)
			}

			input := b.putObjectInput([]byte("data"), "path/to/file")

			if got := aws.StringValue(input.Bucket); got != "bucket" {
				t.Errorf("Bucket = %q, want %q", got, "bucket")
			}
			if got := aws.StringValue(input.Key); got != "prefix/path/to/file" {
				t.Errorf("Key = %q, want %q", got, "prefix/path/to/file")
			}
			if (input.ACL == nil) != (tc.wantAcl == nil) || aws.StringValue(input.ACL) != aws.StringValue(tc.wantAcl) {
				t.Errorf("ACL = %v, want %v", aws.StringValue(input.ACL), aws.StringValue(tc.wantAcl))
			}
			body, err := io.ReadAll(input.Body)
			if err != nil || string(body) != "data" {
				t.Errorf("Body = (%q, %v), want %q", body, err, "data")
			}
		})
	}
}

func TestNewClient_RequiresPrimary(t *testing.T) {
	_, err := NewClient(&config.Config{Buckets: []config.Bucket{{Url: "a.com", Name: "bucket"}}})
	if err == nil {
		t.Errorf("NewClient() succeeded, want error")
	}
}
