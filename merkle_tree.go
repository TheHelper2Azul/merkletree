// Copyright 2017 Cameron Bergoon
// Licensed under the MIT License, see LICENCE file for details.

package merkletree

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
)

// newContent is used for the unified marshalling/unmarshalling of data
// types implementing the Content interface.
var newContent = map[string]func() Content{
	"StorageBucket": func() Content { return new(StorageBucket) },
	"ByteContent":   func() Content { return new(ByteContent) },
}

// Content represents the data that is stored and verified by the tree. A type that
// implements this interface can be used as an item in the tree.
type Content interface {
	CalculateHash() ([]byte, error)
	Equals(other Content) (bool, error)
}

// MerkleTree is the container for the tree. It holds a pointer to the root of the tree,
// a list of pointers to the leaf nodes, and the merkle root.
type MerkleTree struct {
	Root         *Node
	MerkleRoot   []byte
	HashStrategy string
	Leafs        []*Node
}

// GetHashStrategies returns a map which maps the hash strategy name as a string
// to the corresponding hashing function.
func GetHashStrategies() map[string]hash.Hash {
	hashMap := map[string]hash.Hash{
		"sha256": sha256.New(),
	}
	return hashMap
}

// ByteContent enables one to use (root) hashes as merkletree Content
type ByteContent struct {
	Content []byte
}

// Custom marshaler for ByteContent type
func (bc ByteContent) MarshalJSON() ([]byte, error) {
	type _ByteContent ByteContent
	var out = struct {
		Type string `json:"_type"`
		_ByteContent
	}{
		Type:         "ByteContent",
		_ByteContent: _ByteContent(bc),
	}
	return json.Marshal(out)
}

// CalculateHash for ByteContent in order to implement Content.
func (bc ByteContent) CalculateHash() ([]byte, error) {
	return bc.Content, nil
}

// Equals returns true if two ByteContents are identical, false otherwise
func (bc ByteContent) Equals(other Content) (bool, error) {
	if !bytes.Equal(bc.Content, other.(ByteContent).Content) {
		return false, nil
	}
	return true, nil
}

// Node represents a node, root, or leaf in the tree. It stores pointers to its immediate
// relationships, a hash, the content stored if it is a leaf, and other metadata.
type Node struct {
	Left   *Node
	Right  *Node
	Hash   []byte
	C      Content
	tree   *MerkleTree
	parent *Node
	leaf   bool
	Dup    bool
}

// UnmarshalJSON is a custom unmarshaler for nodes
func (n *Node) UnmarshalJSON(byteData []byte) error {
	var node struct {
		Left   *Node
		Right  *Node
		Hash   []byte
		C      json.RawMessage
		tree   *MerkleTree
		parent *Node
		leaf   bool
		Dup    bool
	}
	if err := json.Unmarshal(byteData, &node); err != nil {
		return err
	}
	n.Left = node.Left
	n.Right = node.Right
	n.Hash = node.Hash
	n.tree = node.tree
	n.parent = node.parent
	n.leaf = node.leaf
	n.Dup = node.Dup

	// Check how to cast Content C
	if len(node.C) > 0 && string(node.C) != `null` {

		var _type struct {
			Type string `json:"_type"`
		}
		if err := json.Unmarshal([]byte(node.C), &_type); err != nil {
			return err
		}

		c := newContent[_type.Type]()

		if err := json.Unmarshal([]byte(node.C), c); err != nil {
			return err
		}

		n.C = c

	}
	return nil
}

// // UnmarshalJSON custom unmarshals a node casting Content to StorageBucket
// func (n *Node) UnmarshalJSON(data []byte) error {
// 	var node struct {
// 		Left   *Node
// 		Right  *Node
// 		Hash   []byte
// 		C      StorageBucket
// 		tree   *MerkleTree
// 		parent *Node
// 		leaf   bool
// 		Dup    bool
// 	}
// 	if err := json.Unmarshal(data, &node); err != nil {
// 		return err
// 	}
// 	n.Left = node.Left
// 	n.Right = node.Right
// 	n.Hash = node.Hash
// 	n.C = node.C
// 	n.tree = node.tree
// 	n.parent = node.parent
// 	n.leaf = node.leaf
// 	n.Dup = node.Dup

// 	return nil
// }

//calculateNodeHash is a helper function that calculates the hash of the node.
func (n *Node) calculateNodeHash() ([]byte, error) {
	if n.leaf {
		return n.C.CalculateHash()
	}
	hashMap := GetHashStrategies()
	h := hashMap[n.tree.HashStrategy]
	if _, err := h.Write(append(n.Left.Hash, n.Right.Hash...)); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

//NewTree creates a new Merkle Tree using the content cs.
func NewTree(cs []Content) (*MerkleTree, error) {
	var defaultHashStrategy = "sha256"
	t := &MerkleTree{
		HashStrategy: defaultHashStrategy,
	}
	root, leafs, err := buildWithContent(cs, t)
	if err != nil {
		return nil, err
	}
	t.Root = root
	t.Leafs = leafs
	t.MerkleRoot = root.Hash
	return t, nil
}

// ForestToTree returns a merkle tree made from the root hashes of the trees from @trees
func ForestToTree(trees []MerkleTree) (*MerkleTree, error) {
	var merkleRoots []Content
	for _, tree := range trees {
		merkleRoots = append(merkleRoots, ByteContent{Content: (&tree).MerkleRoot})
	}
	return NewTree(merkleRoots)
}

//NewTreeWithHashStrategy creates a new Merkle Tree using the content cs using the provided hash
//strategy. Note that the hash type used in the type that implements the Content interface must
//match the hash type provided to the tree.
func NewTreeWithHashStrategy(cs []Content, hashStrategy string) (*MerkleTree, error) {
	t := &MerkleTree{
		HashStrategy: hashStrategy,
	}
	root, leafs, err := buildWithContent(cs, t)
	if err != nil {
		return nil, err
	}
	t.Root = root
	t.Leafs = leafs
	t.MerkleRoot = root.Hash
	return t, nil
}

// GetMerklePath gets Merkle path and indexes(left leaf or right leaf)
func (m *MerkleTree) GetMerklePath(content Content) ([][]byte, []int64, error) {
	for _, current := range m.Leafs {
		ok, err := current.C.Equals(content)
		if err != nil {
			return nil, nil, err
		}

		if ok {
			currentParent := current.parent
			var merklePath [][]byte
			var index []int64
			for currentParent != nil {
				if bytes.Equal(currentParent.Left.Hash, current.Hash) {
					merklePath = append(merklePath, currentParent.Right.Hash)
					index = append(index, 1) // right leaf
				} else {
					merklePath = append(merklePath, currentParent.Left.Hash)
					index = append(index, 0) // left leaf
				}
				current = currentParent
				currentParent = currentParent.parent
			}
			return merklePath, index, nil
		}
	}
	return nil, nil, nil
}

//buildWithContent is a helper function that for a given set of Contents, generates a
//corresponding tree and returns the root node, a list of leaf nodes, and a possible error.
//Returns an error if cs contains no Contents.
func buildWithContent(cs []Content, t *MerkleTree) (*Node, []*Node, error) {
	if len(cs) == 0 {
		return nil, nil, errors.New("error: cannot construct tree with no content")
	}
	var leafs []*Node
	for _, c := range cs {
		hash, err := c.CalculateHash()
		if err != nil {
			return nil, nil, err
		}
		leafs = append(leafs, &Node{
			Hash: hash,
			C:    c,
			leaf: true,
			tree: t,
		})
	}
	if len(leafs)%2 == 1 {
		duplicate := &Node{
			Hash: leafs[len(leafs)-1].Hash,
			C:    leafs[len(leafs)-1].C,
			leaf: true,
			Dup:  true,
			tree: t,
		}
		leafs = append(leafs, duplicate)
	}
	root, err := buildIntermediate(leafs, t)
	if err != nil {
		return nil, nil, err
	}

	return root, leafs, nil
}

//buildIntermediate is a helper function that for a given list of leaf nodes, constructs
//the intermediate and root levels of the tree. Returns the resulting root node of the tree.
func buildIntermediate(nl []*Node, t *MerkleTree) (*Node, error) {
	var nodes []*Node

	for i := 0; i < len(nl); i += 2 {
		hashMap := GetHashStrategies()
		h := hashMap[t.HashStrategy]
		var left, right int = i, i + 1
		if i+1 == len(nl) {
			right = i
		}
		chash := append(nl[left].Hash, nl[right].Hash...)
		if _, err := h.Write(chash); err != nil {
			return nil, err
		}
		n := &Node{
			Left:  nl[left],
			Right: nl[right],
			Hash:  h.Sum(nil),
			tree:  t,
		}
		nodes = append(nodes, n)
		nl[left].parent = n
		nl[right].parent = n
		if len(nl) == 2 {
			return n, nil
		}
	}
	return buildIntermediate(nodes, t)
}

//RebuildTree is a helper function that will rebuild the tree reusing only the content that
//it holds in the leaves.
func (m *MerkleTree) RebuildTree() error {
	var cs []Content
	for _, c := range m.Leafs {
		cs = append(cs, c.C)
	}
	root, leafs, err := buildWithContent(cs, m)
	if err != nil {
		return err
	}
	m.Root = root
	m.Leafs = leafs
	m.MerkleRoot = root.Hash
	return nil
}

//RebuildTreeWith replaces the content of the tree and does a complete rebuild; while the root of
//the tree will be replaced the MerkleTree completely survives this operation. Returns an error if the
//list of content cs contains no entries.
func (m *MerkleTree) RebuildTreeWith(cs []Content) error {
	root, leafs, err := buildWithContent(cs, m)
	if err != nil {
		return err
	}
	m.Root = root
	m.Leafs = leafs
	m.MerkleRoot = root.Hash
	return nil
}

// ExtendTree extends the merkle tree @m by the content @cs
func (m *MerkleTree) ExtendTree(cs []Content) error {
	leafs := m.Leafs
	var content []Content
	for _, leaf := range leafs {
		if !leaf.Dup {
			content = append(content, leaf.C)
		}
	}
	content = append(content, cs...)
	err := m.RebuildTreeWith(content)
	return err
}

//verifyNode walks down the tree until hitting a leaf, calculating the hash at each level
//and returning the resulting hash of Node n.
func (n *Node) verifyNode() ([]byte, error) {
	if n.leaf {
		return n.C.CalculateHash()
	}
	rightBytes, err := n.Right.verifyNode()
	if err != nil {
		return nil, err
	}

	leftBytes, err := n.Left.verifyNode()
	if err != nil {
		return nil, err
	}
	hashMap := GetHashStrategies()
	h := hashMap[n.tree.HashStrategy]
	if _, err := h.Write(append(leftBytes, rightBytes...)); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

//VerifyTree verify tree validates the hashes at each level of the tree and returns true if the
//resulting hash at the root of the tree matches the resulting root hash; returns false otherwise.
func (m *MerkleTree) VerifyTree() (bool, error) {
	calculatedMerkleRoot, err := m.Root.verifyNode()
	if err != nil {
		return false, err
	}

	if bytes.Equal(m.MerkleRoot, calculatedMerkleRoot) {
		return true, nil
	}
	return false, nil
}

//VerifyContent indicates whether a given content is in the tree and the hashes are valid for that content.
//Returns true if the expected Merkle Root is equivalent to the Merkle root calculated on the critical path
//for a given content. Returns true if valid and false otherwise.
func (m *MerkleTree) VerifyContent(content Content) (bool, error) {

	for _, l := range m.Leafs {
		ok, err := l.C.Equals(content)
		if err != nil {
			return false, err
		}

		if ok {
			currentParent := l.parent
			for currentParent != nil {
				hashMap := GetHashStrategies()
				h := hashMap[m.HashStrategy]
				rightBytes, err := currentParent.Right.calculateNodeHash()
				if err != nil {
					return false, err
				}

				leftBytes, err := currentParent.Left.calculateNodeHash()
				if err != nil {
					return false, err
				}

				if _, err := h.Write(append(leftBytes, rightBytes...)); err != nil {
					return false, err
				}
				if !bytes.Equal(h.Sum(nil), currentParent.Hash) {
					return false, nil
				}
				currentParent = currentParent.parent
			}
			return true, nil
		}
	}
	return false, nil
}

//String returns a string representation of the node.
func (n *Node) String() string {
	return fmt.Sprintf("%t %t %v %s", n.leaf, n.Dup, n.Hash, n.C)
}

//String returns a string representation of the tree. Only leaf nodes are included
//in the output.
func (m *MerkleTree) String() string {
	s := ""
	for _, l := range m.Leafs {
		s += fmt.Sprint(l)
		s += "\n"
	}
	return s
}

// NumNodes computes the number of nodes in the tree given by the root node @node.
// Leafs are not counted.
func NumNodes(node *Node) int {
	count := 1
	if node.Left.C == nil {
		count += NumNodes(node.Left)
	}
	if node.Right.C == nil {
		count += NumNodes(node.Right)
	}
	return count
}

// Isempty returns true if merkle tree at @m is empty, false otherwise
func (m *MerkleTree) Isempty() bool {
	return m.Root == nil
}

// DataInStorageTree returns true if @data is in a bucket of @tree along with the bucket.
func DataInStorageTree(data []byte, tree MerkleTree) (bool, StorageBucket, error) {
	// oldest date of pool cannot be older than @timestamp as pools are made and stamped after data collection.
	// First look for first pool after @timestamp.
	for _, leaf := range tree.Leafs {
		storageBucket := leaf.C.(StorageBucket)
		content, err := (&storageBucket).ReadContent()
		if err != nil {
			return false, StorageBucket{}, err
		}
		for _, item := range content {
			if bytes.Equal(item, data) {
				return true, storageBucket, nil
			}
		}
	}
	return false, StorageBucket{}, nil
}
