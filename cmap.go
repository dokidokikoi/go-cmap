package cmap

import (
	"math"
	"sync/atomic"
)

// 并发安全 map 的接口
type ConcurrentMap interface {
	// 用于返回并发量
	Concurrency() int
	// 注意！element 不能为 nil
	// 第一个返回值表示是否新增了键值对
	// 若键已存在，新元素将替换旧元素
	Put(key string, element interface{}) (bool, error)
	// 若返回 nil 说明键不存在
	Get(key string) interface{}
	// 删除指定键值对
	// 不存在返回 false
	Delete(key string) bool
	// 返回键值对数量
	Len() uint64
}

type myConcurrentMap struct {
	// 并发量，也代表了 segments 的长度
	concurrency int
	// 一个 segment 代表一个散列值
	// 分段锁保证并发安全
	// 长度在初始化是就需要确定且不可更改
	segments []Segment
	// 键值对数量
	total uint64
}

func (c *myConcurrentMap) Concurrency() int {
	return c.concurrency
}

func (c *myConcurrentMap) Put(key string, element interface{}) (bool, error) {
	p, err := newPair(key, element)
	if err != nil {
		return false, err
	}
	s := c.findSegment(p.Hash())
	ok, err := s.Put(p)
	if ok {
		atomic.AddUint64(&c.total, 1)
	}
	return ok, err
}

// 根据给定参数寻找并返回对应散列段
// 使用高位的几个字节来决定散列段的索引
// 可以使键值对在 segments 中分布更广更均匀
func (c *myConcurrentMap) findSegment(keyHash uint64) Segment {
	if c.concurrency == 1 {
		return c.segments[0]
	}
	var keyHash32 uint32
	if keyHash > math.MaxUint32 {
		keyHash32 = uint32(keyHash >> 32)
	} else {
		keyHash32 = uint32(keyHash)
	}

	return c.segments[int(keyHash32>>16)%(c.concurrency-1)]
}

func (c *myConcurrentMap) Get(key string) interface{} {
	keyHash := hash(key)
	s := c.findSegment(keyHash)
	pair := s.GetWithHash(key, keyHash)
	if pair == nil {
		return nil
	}

	return pair.Element()
}

func (c *myConcurrentMap) Delete(key string) bool {
	s := c.findSegment(hash(key))
	if s.Delete(key) {
		atomic.AddUint64(&c.total, ^uint64(0))
		return true
	}
	return false
}

func (cmap *myConcurrentMap) Len() uint64 {
	return atomic.LoadUint64(&cmap.total)
}

// 参数 pairRedistributor 可以为空
func NewConcurrentMap(concurrency int, pairRedistributor PairRedistributor) (ConcurrentMap, error) {
	if concurrency <= 0 {
		return nil, newIllegalParameterError("concurrency is too small")
	}
	if concurrency > MAX_CONCURRENCY {
		return nil, newIllegalParameterError("concurrency is too large")
	}
	cmap := &myConcurrentMap{}
	cmap.concurrency = concurrency
	cmap.segments = make([]Segment, concurrency)
	for i := 0; i < concurrency; i++ {
		cmap.segments[i] = newSegment(DEFAULT_BUCKET_NUMBER, pairRedistributor)
	}
	return cmap, nil
}
