package lineage

import (
	"reflect"
	"testing"
)

func TestDAGWithMergeNode(t *testing.T) {
	dag := NewDAG().WithDebug()

	// Build initial tree (root (A (C D)) B))
	dag.AddEdge("", "root")
	dag.AddEdge("root", "A")
	dag.AddEdge("root", "B")
	dag.AddEdge("A", "C")
	dag.AddEdge("A", "D")

	// Add merge node M with parents (B C D)
	dag.CreateMergeNode([]string{"B", "C", "D"}, "M")
	dag.AddEdge("M", "E")

	lineageC := dag.GetLineage("C")
	expectedC := []string{"C", "A", "root", "M", "E"}
	if !reflect.DeepEqual(lineageC, expectedC) {
		t.Errorf("Lineage of C: got %v, want %v", lineageC, expectedC)
	}

	lineageM := dag.GetLineage("M")
	expectedM := []string{"M", "D", "A", "root", "C", "B", "E"}
	if !reflect.DeepEqual(lineageM, expectedM) {
		t.Errorf("Lineage of M: got %v, want %v", lineageM, expectedM)
	}
}
