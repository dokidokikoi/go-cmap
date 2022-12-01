package cmap

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// 用来表示并发安全对散列段的接口
type Segment interface {
	// 根据参数放入一个键值对
	// 第一个返回值表示是否新增成功
	Put(p Pair) (bool, error)
	// 根据参数返回一个键值对
	Get(key string) Pair
	// 根据参数返回一个键值对
	// 注意！参数 keyHash 是基于key计算出的散列值
	// 主要为了避免重复计算键的散列值
	GetWithHash(key string, keyHash uint64) Pair
	// 删除指定参数的键值对
	Delete(key string) bool
	// 获取当前段段尺寸(其中包含的散列桶的数量)
	Size() uint64
}

// 用于表示并发安全的散列段的类型
type segment struct {
	// 用于表示散列桶切片
	buckets []Bucket
	// 用于表示散列桶切片的长度
	bucketsLen int
	// 用于表示键值对总数
	pairTotal uint64
	// 用于表示键值对的再分布器
	pairRedistributor PairRedistributor
	lock              sync.Mutex
}

// 用于检查给定参数并设置相应的阈值和计数
// 并在必要时重新分配所有散列桶中所有的键值对
// 注意！必须在互斥锁的保护下调用该方法
func (s *segment) redistribute(pairTotal uint64, bucketSize uint64) (err error) {
	// 防止该方法出现 panic
	defer func() {
		if p := recover(); p != nil {
			if pErr, ok := p.(error); ok {
				err = newPairRedistributorError(pErr.Error())
			} else {
				err = newPairRedistributorError(fmt.Sprintf("%s", p))
			}
		}
	}()

	s.pairRedistributor.UpdateThreshold(pairTotal, s.bucketsLen)
	bucketStatus := s.pairRedistributor.CheckBucketStatus(pairTotal, bucketSize)
	newBuckets, changed := s.pairRedistributor.Redistribe(bucketStatus, s.buckets)
	if changed {
		s.buckets = newBuckets
		s.bucketsLen = len(s.buckets)
	}

	return nil
}

func (s *segment) Put(p Pair) (bool, error) {
	s.lock.Lock()
	b := s.buckets[int(p.Hash()%uint64(s.bucketsLen))]
	ok, err := b.Put(p, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.pairTotal, 1)
		s.redistribute(newTotal, b.Size())
	}
	s.lock.Lock()
	return ok, err
}

func (s *segment) Get(key string) Pair {
	return s.GetWithHash(key, hash(key))
}

func (s *segment) GetWithHash(key string, keyHash uint64) Pair {
	s.lock.Lock()
	b := s.buckets[int(keyHash%uint64(s.bucketsLen))]
	s.lock.Unlock()
	return b.Get(key)
}

func (s *segment) Delete(key string) bool {
	s.lock.Lock()
	b := s.buckets[int(hash(key)%uint64(s.bucketsLen))]
	ok := b.Delete(key, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.pairTotal, ^uint64(0))
		s.redistribute(newTotal, b.Size())
	}
	s.lock.Unlock()

	return ok
}

func (s *segment) Size() uint64 {
	return atomic.LoadUint64(&s.pairTotal)
}

func newSegment(bucketNumber int, pairRedistributor PairRedistributor) Segment {
	if bucketNumber < 0 {
		bucketNumber = DEFAULT_BUCKET_NUMBER
	}
	if pairRedistributor == nil {
		pairRedistributor = newDefaultPairRedistributor(DEFAULT_BUCKET_LOAD_FACTOR, bucketNumber)
	}
	buckets := make([]Bucket, bucketNumber)
	for i := 0; i < bucketNumber; i++ {
		buckets[i] = newBucket()
	}

	return &segment{
		buckets:           buckets,
		bucketsLen:        bucketNumber,
		pairRedistributor: pairRedistributor,
	}
}
