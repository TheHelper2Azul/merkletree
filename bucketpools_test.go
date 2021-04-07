package merkletree

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestBucketpools_CalculateHash(t *testing.T) {
	content1 := []byte("")
	content2 := []byte("Hello.kkk")
	content3 := []byte("779 3")

	bucket1 := NewBucket(uint64(16), "test1")
	h1 := sha256.New()
	h1.Write(content1)
	hash1 := h1.Sum(nil)

	bucket2 := NewBucket(uint64(2), "test2")
	bucket2.Content.Write(content2)
	h2 := sha256.New()
	h2.Write(content2)
	hash2 := h2.Sum(nil)

	bucket3 := NewBucket(uint64(128), "test3")
	bucket3.Content.Write(content3)
	h3 := sha256.New()
	h3.Write(content3)
	hash3 := h3.Sum(nil)

	tables := []struct {
		bucket *Bucket
		hash   []byte
	}{
		{bucket1, hash1},
		{bucket2, hash2},
		{bucket3, hash3},
	}
	for _, table := range tables {
		value, _ := table.bucket.CalculateHash()
		if !bytes.Equal(value, table.hash) {
			t.Errorf("Value of %v was incorrect, got: %v, want: %v.", table.bucket, value, table.hash)
		}
	}
}

func TestBucketpools_Get(t *testing.T) {
	// TO DO: More test cases
	size1 := uint64(0)
	bp1 := NewBucketPool(0, size1, "test1")

	size2 := uint64(16)
	bp2 := NewBucketPool(2, size2, "test2")

	tables := []struct {
		pool   *BucketPool
		bucket Bucket
		err    bool
	}{
		{
			pool:   bp1,
			bucket: Bucket{Content: bytes.NewBuffer(make([]byte, 0, size1)), Topic: "test2", size: size1},
			err:    true,
		},
		{
			pool:   bp2,
			bucket: Bucket{Content: bytes.NewBuffer(make([]byte, 0, size2)), Topic: "test2", size: size2},
			err:    false,
		},
	}
	for _, table := range tables {
		bucket, errValue := table.pool.Get()
		// Set err to true if error is not nil
		var err bool
		if errValue != nil {
			err = true
		}
		if bucket.size != table.bucket.size {
			t.Errorf("size of bucket incorrect: got %v but expected %v", bucket.size, table.bucket.size)
		}
		if err != table.err {
			t.Errorf("error was incorrect. got %v but expected %v", err, table.err)
		}
	}
}

func TestBucketpools_Put(t *testing.T) {
	size1 := uint64(16)
	bp1 := NewBucketPool(2, size1, "test1")

	size2 := uint64(32)
	bp2 := NewBucketPool(2, size2, "test2")
	<-bp2.c

	tables := []struct {
		pool    *BucketPool
		bucket  Bucket
		success bool
	}{
		{
			pool:    bp1,
			bucket:  Bucket{Content: bytes.NewBuffer(make([]byte, 0, size1)), Topic: "test2", size: size1},
			success: false,
		},
		{
			pool:    bp1,
			bucket:  Bucket{Content: bytes.NewBuffer(make([]byte, 0, size2)), Topic: "test1", size: size2},
			success: false,
		},
		{
			pool:    bp2,
			bucket:  Bucket{Content: bytes.NewBuffer(make([]byte, 0, size2)), Topic: "test2", size: size2},
			success: true,
		},
	}
	for _, table := range tables {
		success := table.pool.Put(table.bucket)
		if success != table.success {
			t.Errorf("error: got %v but expected %v", success, table.success)
		}
	}
}

func TestBucketpools_WriteContent(t *testing.T) {

	tables := []struct {
		bucket  *Bucket
		content []byte
		success bool
	}{
		{
			bucket:  NewBucket(uint64(16), ""),
			content: []byte("too long."),
			success: false,
		},
		{
			bucket:  NewBucket(uint64(16), ""),
			content: []byte("ok"),
			success: true,
		},
	}

	for _, table := range tables {
		success := table.bucket.WriteContent(table.content)
		if success != table.success {
			t.Errorf("error: got %v but expected %v", success, table.success)
		}
	}
}

func TestBucketpools_ReadContent(t *testing.T) {
	bucket1 := NewBucket(uint64(256), "test")
	teststrings := []string{
		"Testing",
		"  ",
		" ReadContent method.",
		"",
		"with more \n élements ",
	}
	for _, teststring := range teststrings {
		bucket1.WriteContent([]byte(teststring))
	}
	storageBucket1 := bucketToStorage(*bucket1)

	content, _ := storageBucket1.ReadContent()
	for i := 0; i < len(content); i++ {
		if string(content[i]) != teststrings[i] {
			t.Errorf("error: got %s but expected %s", content[i], teststrings[i])
		}
	}
}
