package merkletree

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	log "github.com/sirupsen/logrus"
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
	size     uint64
	// values possibly assigned to the bucket
	ID string
	// Timestamp is the time, the filled bucket is put into the pool
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
	if !bytes.Equal(b.Content.Bytes(), other.(Bucket).Content.Bytes()) {
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
	Content []byte
	// TO DO: make HashRate and Size dependent on Topic?
	Topic     string
	HashRate  time.Duration
	Size      uint64
	ID        string
	Timestamp time.Time
}

// Custom marshaler for StorageBucket type
func (sb StorageBucket) MarshalJSON() ([]byte, error) {
	type _StorageBucket StorageBucket
	var out = struct {
		Type string `json:"_type"`
		_StorageBucket
	}{
		Type:           "StorageBucket",
		_StorageBucket: _StorageBucket(sb),
	}
	return json.Marshal(out)
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
	if !bytes.Equal(sb.Content, other.(*StorageBucket).Content) {
		return false, nil
	}
	if sb.Size != other.(*StorageBucket).Size {
		return false, nil
	}
	if sb.ID != other.(*StorageBucket).ID {
		return false, nil
	}
	if sb.Topic != other.(*StorageBucket).Topic {
		return false, nil
	}
	if sb.HashRate != other.(*StorageBucket).HashRate {
		return false, nil
	}
	return true, nil
}

// bucketToStorage converts a bucket to a StorageBucket, ready for marshaling for influx.
func bucketToStorage(b Bucket) (sb StorageBucket) {

	sb.Content = b.Content.Bytes()
	sb.Topic = b.Topic
	sb.HashRate = b.HashRate
	sb.Size = b.size
	sb.ID = b.ID
	sb.Timestamp = b.Timestamp

	return
}

// BucketPool implements a leaky pool of Buckets in the form of a bounded channel.
type BucketPool struct {
	c     chan Bucket
	width uint64
	Topic string
}

// NewBucket creates a new bucket of size @size in bytes.
func NewBucket(size uint64, topic string) (b *Bucket) {
	return &Bucket{
		Content: bytes.NewBuffer(make([]byte, 0, size)),
		size:    size,
		Topic:   topic,
	}
}

// NewBucketPool creates a new BucketPool bounded to the length @maxNum.
// It is initialized with empty Buckets of capacity @size.
func NewBucketPool(maxNum uint64, size uint64, topic string) (bp *BucketPool) {
	bp = &BucketPool{
		c:     make(chan Bucket, maxNum),
		width: size,
		Topic: topic,
	}
	// Fill channel with empty buckets
	for i := 0; i < int(maxNum); i++ {
		bucket := NewBucket(size, topic)
		bp.c <- *bucket
	}
	return
}

// Size returns the size of a bucket
func (b *Bucket) Size() uint64 {
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
		log.Error("topic error: only one topic per pool.")
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

// WriteContent appends a byte slice to a bucket if there is enough space.
// Does not write and returns false if there isn't.
// Contents are separated by leading 64bit unsigned integers.
func (b *Bucket) WriteContent(bs []byte) bool {
	if b.Content.Len()+len(bs)+8 > int(b.Size()) {
		return false
	}
	// Store length of content as 8-byte array
	lenPrefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(lenPrefix, uint64(len(bs)))
	// Write length and content
	b.Content.Write(lenPrefix)
	b.Content.Write(bs)

	b.used = true
	return true

}

// ReadContent returns the content of a storage bucket.
// Each byte slice correponds to a marshaled data point such as an
// interest rate or a trade.
func (sb *StorageBucket) ReadContent() (data [][]byte, err error) {
	buf := bytes.NewBuffer(sb.Content)
	readOn := true
	for readOn {
		// get length of content byte slice by reading the prefix
		lenPrefix := make([]byte, 8)
		buf.Read(lenPrefix)
		lenContent := binary.LittleEndian.Uint64(lenPrefix)
		if lenContent > 0 {
			// In case there is content read it...
			content := make([]byte, lenContent)
			_, err = buf.Read(content)
			data = append(data, [][]byte{content}...)
		} else {
			// ...otherwise stop reading
			readOn = false
		}
	}
	return

}

// MakeTree returns a Merkle tree built from the Buckets in the pool @bp
func MakeTree(bp *BucketPool) (*MerkleTree, error) {
	leafs := []Content{}
	numBuckets := bp.Len()
	for i := 0; i < numBuckets; i++ {
		storageBucket := bucketToStorage(<-bp.c)
		leafs = append(leafs, storageBucket)
	}
	t, err := NewTree(leafs)
	return t, err
}
