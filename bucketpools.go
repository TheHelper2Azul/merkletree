package merkletree

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

// -----------------------------------------------------------------------
// Bucket functionality
// -----------------------------------------------------------------------

// Bucket implements Content interface from merkletree package.
// @Content is a byte slice of fixed size per @Type
// @ID is a unique identification string
// @Type designates the category of data (for instance interest rate or trade)
// @HashRate sets the frequency of hashing the data of the corresponding type
// @Timestamp is the (Unix-?)time, the container is hashed
type Bucket struct {
	Content *bytes.Buffer
	// properties of the bucket
	Topic    string
	HashRate time.Duration
	size     int
	// values possibly assigned to the bucket
	ID        string
	Timestamp time.Time
	used      bool
}

// TODO: These two methods can be removed: Bucket does not have to implement Content,
// because we build the trees from StorageBuckets.

// CalculateHash calculates the hash of a bucket. Is needed for a bucket in
// order to implement Content from merkle_tree.
func (b Bucket) CalculateHash() ([]byte, error) {
	h := sha256.New()
	if _, err := h.Write(b.Content.Bytes()); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// Equals is true if buckets are identical. Is needed for a bucket in
// order to implement Content from merkle_tree.
func (b Bucket) Equals(other Content) (bool, error) {
	// Extend for other fields, but which? Do we need all fields?
	if !EqualBytes(b.Content.Bytes(), other.(Bucket).Content.Bytes()) {
		return false, nil
	}
	if b.size != other.(Bucket).size {
		return false, nil
	}
	if b.ID != other.(Bucket).ID {
		return false, nil
	}
	if b.Topic != other.(Bucket).Topic {
		return false, nil
	}
	if b.HashRate != other.(Bucket).HashRate {
		return false, nil
	}
	return true, nil
}

// StorageBucket is similar to a bucket.
// In contrast to bucket it is only used for storage in influx and read, not for write.
type StorageBucket struct {
	Content   []byte
	Topic     string
	HashRate  time.Duration
	size      int
	ID        string
	Timestamp time.Time
}

// CalculateHash calculates the hash of a StorageBucket. Is needed for a StorageBucket in
// order to implement Content from merkle_tree.
func (sb StorageBucket) CalculateHash() ([]byte, error) {
	h := sha256.New()
	if _, err := h.Write(sb.Content); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

// Equals is true if StorageBuckets are identical. Is needed for a StorageBucket in
// order to implement Content from merkle_tree.
func (sb StorageBucket) Equals(other Content) (bool, error) {
	// Extend for other fields, but which? Do we need all fields?
	if !EqualBytes(sb.Content, other.(StorageBucket).Content) {
		return false, nil
	}
	if sb.size != other.(StorageBucket).size {
		return false, nil
	}
	if sb.ID != other.(StorageBucket).ID {
		return false, nil
	}
	if sb.Topic != other.(StorageBucket).Topic {
		return false, nil
	}
	if sb.HashRate != other.(StorageBucket).HashRate {
		return false, nil
	}
	return true, nil
}

// bucketToStorage converts a bucket to a StorageBucket, ready for marshaling for influx.
func bucketToStorage(b Bucket) (sb StorageBucket) {

	sb.Content = b.Content.Bytes()
	sb.Topic = b.Topic
	sb.HashRate = b.HashRate
	sb.size = b.size
	sb.ID = b.ID
	sb.Timestamp = b.Timestamp

	return
}

// BucketPool implements a leaky pool of Buckets in the form of a bounded channel.
type BucketPool struct {
	c     chan Bucket
	width int
	Topic string
}

// NewBucket creates a new bucket of size @size.
// TO DO: Extend to Type of Bucket (and pool below)
func NewBucket(size int, topic string) (b *Bucket) {
	return &Bucket{
		Content: bytes.NewBuffer(make([]byte, 0, size)),
		size:    size,
		Topic:   topic,
	}
}

// NewBucketPool creates a new BucketPool bounded to the given maxSize.
// It is initialized with empty Buckets sized based on width.
func NewBucketPool(maxNum int, size int, topic string) (bp *BucketPool) {
	bp = &BucketPool{
		c:     make(chan Bucket, maxNum),
		width: size,
		Topic: topic,
	}
	// Fill channel with empty buckets
	for i := 0; i < maxNum; i++ {
		bucket := NewBucket(size, topic)
		bp.c <- *bucket
	}
	return
}

// Size returns the size of a bucket
func (b *Bucket) Size() int {
	return b.size
}

// Used returns true if the bucket was written to
func (b *Bucket) Used() bool {
	return b.used
}

// Len returns the numbers of elements in the bucket pool
func (bp *BucketPool) Len() int {
	return len(bp.c)
}

// Get gets a Bucket from the BucketPool, or creates a new one if none are
// available in the pool.
func (bp *BucketPool) Get() (b Bucket, err error) {
	select {
	case b = <-bp.c:
		// Get bucket from pool

		if b.used {
			// In this case, all buckets from the pool have been used and a new pool
			// should be created
			bp.c <- b
			return *NewBucket(bp.width, bp.Topic), errors.New("size error. pool is exhausted")
		}
	default:
		return *NewBucket(bp.width, bp.Topic), errors.New("size error. pool is exhausted")
		// fmt.Println("make new bucket")
		// b = *NewBucket(bp.width)
	}
	return
}

// Put returns the given Bucket to the BucketPool.
func (bp *BucketPool) Put(b Bucket) bool {
	// Check whether Bucket is of the right kind. If not, reject.
	if bp.Topic != b.Topic {
		fmt.Println("error with topics")
		return false
	}

	select {
	case bp.c <- b:
		// Bucket went back into pool.
		return true
	default:
		// Bucket didn't go back into pool, just discard.
		return false
	}
}

// WriteContent appends a byte slice to a bucket if there is enough
// space. Does not write and returns false if there isn't.
func (b *Bucket) WriteContent(bs []byte) bool {
	if b.Content.Len()+len(bs) > b.Size() {
		return false
	}
	b.Content.Write(bs)
	// Separate content by newline for later data retrieval.
	if b.Content.Len() < b.Size() {
		fmt.Println("newline added")
		b.Content.Write([]byte("\n"))
	}
	b.used = true
	return true
}

// ReadContent returns the content of a storage bucket.
// Each byte slice correponds to a marshaled data point such as an InterestRate.
func (sb *StorageBucket) ReadContent() (data [][]byte, err error) {
	buf := bytes.NewBuffer(sb.Content)
	eof := false
	for !eof {
		item, err := buf.ReadBytes(byte('\n'))

		if err == nil {
			data = append(data, [][]byte{item[0 : len(item)-1]}...)
		} else {
			if err.Error() == "EOF" {
				if len(item) > 0 {
					// This case occurs when "\n" is exactly the last byte in the bucket
					data = append(data, [][]byte{item}...)
				}
				eof = true
			} else {
				log.Error("error in reading bytes buffer: ", err)
				return [][]byte{}, err
			}
		}

	}
	return
}

// MakeTree returns a Merkle tree built from the Buckets in the pool @bp
func MakeTree(bp *BucketPool) (*MerkleTree, error) {
	leafs := []Content{}
	L := bp.Len()
	for i := 0; i < L; i++ {
		storageBucket := bucketToStorage(<-bp.c)
		leafs = append(leafs, storageBucket)
	}
	t, err := NewTree(leafs)
	return t, err
}

// EqualBytes compares two byte slices. Should be put into some helper package.
func EqualBytes(a, b []byte) bool {
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
