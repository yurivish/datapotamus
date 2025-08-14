package lineage

import "fmt"

type Node struct {
	ID       string
	Parents  []*Node
	Children []*Node
}

type DAG struct {
	nodes map[string]*Node
}

func NewDAG() *DAG {
	return &DAG{
		nodes: make(map[string]*Node),
	}
}

func (d *DAG) AddEdge(id, parentID string) {
	// Get or create child node
	child := d.nodes[id]
	if child == nil {
		child = &Node{ID: id}
		d.nodes[id] = child
	}

	// Root node case (no parent)
	if parentID == "" {
		return
	}

	// Get or create parent node
	parent := d.nodes[parentID]
	if parent == nil {
		parent = &Node{ID: parentID}
		d.nodes[parentID] = parent
	}

	// Check if edge already exists to avoid duplicates
	for _, p := range child.Parents {
		if p.ID == parentID {
			return // Edge already exists
		}
	}

	// Link bidirectionally
	parent.Children = append(parent.Children, child)
	child.Parents = append(child.Parents, parent)
}

func traverseDAG(starts []*Node, visited map[string]bool, getNext func(*Node) []*Node) []string {
	var result []string
	stack := []*Node{}

	// Initialize stack with unvisited starting nodes
	for _, node := range starts {
		if node != nil && !visited[node.ID] {
			stack = append(stack, node)
			visited[node.ID] = true
		}
	}

	// Traverse (DFS-style due to stack, but visits all nodes regardless)
	for len(stack) > 0 {
		// Pop from end (efficient!)
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Process node
		result = append(result, node.ID)

		// Push unvisited neighbors
		for _, next := range getNext(node) {
			if next != nil && !visited[next.ID] {
				stack = append(stack, next)
				visited[next.ID] = true
			}
		}
	}

	return result
}

func (d *DAG) GetLineage(queryID string) []string {
	query := d.nodes[queryID]
	if query == nil {
		return nil
	}

	visited := make(map[string]bool)
	visited[queryID] = true

	// Get ancestors by traversing up through parents
	ancestors := traverseDAG(query.Parents, visited, func(n *Node) []*Node {
		return n.Parents
	})

	// Get descendants by traversing down through children
	descendants := traverseDAG(query.Children, visited, func(n *Node) []*Node {
		return n.Children
	})

	// Combine results
	return append(ancestors, descendants...)
}

// Helper: Create a merge node that groups multiple nodes
func (d *DAG) CreateMergeNode(mergeID string, nodeIDs ...string) {
	// Ensure all nodes exist first
	for _, nodeID := range nodeIDs {
		if d.nodes[nodeID] == nil {
			panic(fmt.Sprintf("Node %s must exist before creating merge node", nodeID))
		}
	}

	// Create merge node and add edges
	for _, nodeID := range nodeIDs {
		d.AddEdge(mergeID, nodeID)
	}
}
