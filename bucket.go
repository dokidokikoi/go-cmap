// 不同字符串的散列值可能相同，这种现象称之为“哈希碰撞”
// 桶就是用来存放散列值相同的键值对的
package cmap

import (
	"bytes"
	"sync"
	"sync/atomic"
)

// 并发安全的散列桶接口
type Bucket interface {
	// 若在调用次方法前已经加了锁，则不要把锁传入！否则必须传入 lock
	Put(p Pair, lock sync.Locker) (bool, error)
	Get(key string) Pair
	// 返回第一个键值对
	GetFirstPair() Pair
	// 若在调用次方法前已经加了锁，则不要把锁传入！否则必须传入 lock
	Delete(key string, lock sync.Locker) bool
	// 清空当前散列桶
	// 若在调用次方法前已经加了锁，则不要把锁传入！否则必须传入 lock
	Clear(lock sync.Locker)
	// 返回当前散列值的尺寸
	Size() uint64
	// 返回当前桶的字符串形式
	String() string
}

// 由于 atomic.Value 不能存放 nil，
// 使用 placeholder 表示桶为空
var placeholder Pair = &pair{}

type bucket struct {
	// 存放键值对列表的表头
	firstValue atomic.Value
	size       uint64
}

func (b *bucket) GetFirstPair() Pair {
	if v := b.firstValue.Load(); v == nil {
		return nil
	} else if p, ok := v.(Pair); !ok || p == placeholder {
		return nil
	} else {
		return p
	}
}

func (b *bucket) Put(p Pair, lock sync.Locker) (bool, error) {
	if p == nil {
		return false, newIllegalParameterError("pair is nil")
	}
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}

	firstPair := b.GetFirstPair()
	if firstPair == nil {
		b.firstValue.Store(p)
		atomic.AddUint64(&b.size, 1)
		return true, nil
	}
	var target Pair
	key := p.Key()
	for v := firstPair; v != nil; v = v.Next() {
		if v.Key() == key {
			target = v
			break
		}
	}
	if target != nil {
		target.SetElement(p.Element())
		return false, nil
	}
	// 新加入的键值对做表头，因此可以并发安全的 get 键值对
	p.SetNext(firstPair)
	b.firstValue.Store(p)
	atomic.AddUint64(&b.size, 1)

	return true, nil
}

// Delete 同样可以并发安全的 get 键值对
// 删除逻辑：将要删除目标键值对的前置节点拷贝，
// 再接在目标键值对后一个节点前
func (b *bucket) Delete(key string, lock sync.Locker) bool {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	firstPair := b.GetFirstPair()
	if firstPair == nil {
		return false
	}

	var prevPairs []Pair
	var target Pair
	var breakpoint Pair
	for v := firstPair; v != nil; v = v.Next() {
		if v.Key() == key {
			target = v
			breakpoint = v.Next()
			break
		}
		prevPairs = append(prevPairs, v)
	}
	if target == nil {
		return false
	}
	newFirstPair := breakpoint
	for i := len(prevPairs) - 1; i >= 0; i-- {
		pairCopy := prevPairs[i].Copy()
		pairCopy.SetNext(newFirstPair)
		newFirstPair = pairCopy
	}
	if newFirstPair != nil {
		b.firstValue.Store(newFirstPair)
	} else {
		b.firstValue.Store(placeholder)
	}
	atomic.AddUint64(&b.size, ^uint64(0))

	return true
}

func (b *bucket) Get(key string) Pair {
	firstPair := b.GetFirstPair()
	if firstPair == nil {
		return nil
	}
	for v := firstPair; v != nil; v = v.Next() {
		if v.Key() == key {
			return v
		}
	}

	return nil
}

func (b *bucket) Clear(lock sync.Locker) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	atomic.StoreUint64(&b.size, 0)
	b.firstValue.Store(placeholder)
}

func (b *bucket) Size() uint64 {
	return atomic.LoadUint64(&b.size)
}

func (b *bucket) String() string {
	var buf bytes.Buffer
	buf.WriteString("[ ")
	for v := b.GetFirstPair(); v != nil; v = v.Next() {
		buf.WriteString(v.String())
		buf.WriteString(" ")
	}
	buf.WriteString("]")
	return buf.String()
}

func newBucket() Bucket {
	b := &bucket{}
	b.firstValue.Store(placeholder)
	return b
}
