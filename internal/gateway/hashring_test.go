package gateway

import (
	"math/rand"
	"testing"
)

func TestHashRingGetConsistent(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	hr.Add("gw-2")
	hr.Add("gw-3")

	// 相同的键应始终返回相同的节点。
	first := hr.Get("alice")
	if first == "" {
		t.Fatal("Get on non-empty ring returned empty")
	}
	for i := 0; i < 100; i++ {
		if got := hr.Get("alice"); got != first {
			t.Errorf("Get(\"alice\") inconsistent: first=%s, iteration %d=%s", first, i, got)
		}
	}
}

func TestHashRingGetDistribution(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	hr.Add("gw-2")
	hr.Add("gw-3")

	counts := map[string]int{"gw-1": 0, "gw-2": 0, "gw-3": 0}
	const n = 3000

	for i := 0; i < n; i++ {
		uid := "user-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		node := hr.Get(uid)
		if node == "" {
			t.Fatal("Get returned empty for non-empty ring")
		}
		counts[node]++
	}

	// 每个节点 150 个虚拟节点、3000 个键时，每个节点应获得
	// 约 33%（1000 个键）。允许 25%-42% 的容差。
	for node, count := range counts {
		pct := float64(count) / float64(n) * 100
		if pct < 25 || pct > 42 {
			t.Errorf("node %s got %.1f%% of keys, expected ~33%% (25-42%% acceptable)", node, pct)
		}
	}
}

func TestHashRingEmpty(t *testing.T) {
	hr := NewHashRing(150)
	if got := hr.Get("alice"); got != "" {
		t.Errorf("Get on empty ring expected \"\", got %q", got)
	}
	if hr.Len() != 0 {
		t.Errorf("Len on empty ring expected 0, got %d", hr.Len())
	}
}

func TestHashRingSingleNode(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("only-node")

	if hr.Len() != 1 {
		t.Errorf("Len expected 1, got %d", hr.Len())
	}

	for i := 0; i < 100; i++ {
		uid := "user-" + string(rune('a'+i%26))
		if got := hr.Get(uid); got != "only-node" {
			t.Errorf("Get(%q) on single-node ring expected \"only-node\", got %q", uid, got)
		}
	}
}

func TestHashRingAddAndRemove(t *testing.T) {
	hr := NewHashRing(150)

	// 2 个节点：所有键均匀分布。
	hr.Add("gw-1")
	hr.Add("gw-2")
	if hr.Len() != 2 {
		t.Errorf("Len expected 2, got %d", hr.Len())
	}

	// 移除 gw-2：所有键 → gw-1。
	hr.Remove("gw-2")
	if hr.Len() != 1 {
		t.Errorf("Len after remove expected 1, got %d", hr.Len())
	}
	for i := 0; i < 50; i++ {
		uid := "user-" + string(rune('a'+i%26))
		if got := hr.Get(uid); got != "gw-1" {
			t.Errorf("Get(%q) after removing gw-2 expected \"gw-1\", got %q", uid, got)
		}
	}

	// 重新添加 gw-2：键重新分布。
	hr.Add("gw-2")
	if hr.Len() != 2 {
		t.Errorf("Len after re-add expected 2, got %d", hr.Len())
	}
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		uid := "user-" + string(rune('a'+i%26))
		seen[hr.Get(uid)] = true
	}
	if !seen["gw-1"] || !seen["gw-2"] {
		t.Error("re-adding gw-2 did not redistribute keys")
	}
}

func TestHashRingRemoveNonExistent(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	// 不应 panic。
	hr.Remove("nonexistent")
	if hr.Len() != 1 {
		t.Errorf("Len expected 1, got %d", hr.Len())
	}
}

func TestHashRingReAdd(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	first := hr.Get("alice")

	// 重新添加相同的节点应具有幂等性。
	hr.Add("gw-1")
	if got := hr.Get("alice"); got != first {
		t.Errorf("Re-adding same node changed routing: %s → %s", first, got)
	}
	if hr.Len() != 1 {
		t.Errorf("Len after re-add expected 1, got %d", hr.Len())
	}
}

func TestHashRingDefaultReplicas(t *testing.T) {
	hr := NewHashRing(0) // 0 → 默认 150
	if hr.replicas != 150 {
		t.Errorf("default replicas expected 150, got %d", hr.replicas)
	}

	hr2 := NewHashRing(-1)
	if hr2.replicas != 150 {
		t.Errorf("default replicas expected 150, got %d", hr2.replicas)
	}

	hr3 := NewHashRing(50)
	if hr3.replicas != 50 {
		t.Errorf("custom replicas expected 50, got %d", hr3.replicas)
	}
}

// 测试在键查找期间添加节点不会引起
// 问题（无数据竞争等）。
func TestHashRingConcurrentAddAndGet(t *testing.T) {
	hr := NewHashRing(150)
	hr.Add("gw-1")
	hr.Add("gw-2")

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			hr.Add("gw-" + string(rune('a'+i%5)))
			hr.Remove("gw-" + string(rune('a'+(i+2)%5)))
		}
		close(done)
	}()

	for i := 0; i < 1000; i++ {
		uid := "user-" + string(rune('a'+i%26))
		_ = hr.Get(uid)
		_ = hr.Len()
	}

	<-done
}

func init() {
	// 为分布测试设置随机种子。
	_ = rand.Int()
}
