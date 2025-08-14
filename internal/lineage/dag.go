package lineage

import "fmt"

type Node struct {
	ID       string
	Parents  []*Node
	Children []*Node
}

// This DAG type is designed to represent a tree with structural sharing,
// Which is what our flow system emits in terms of message relationships
// through id and parent ID. For the most part, messages are derived from
// other messages and form a tree. But sometimes we create a merge node
// from multiple messages, such as when messages are batched, and assign
// that its own ID that can then be used as the parent for further messages.
type DAG struct {
	nodes map[string]*Node
	debug bool // enable expensive assertions
}

func NewDAG() *DAG {
	return &DAG{
		nodes: map[string]*Node{},
		debug: false,
	}
}

func (d *DAG) WithDebug() *DAG {
	d.debug = true
	return d
}

func (d *DAG) AddEdge(parentID, id string) {
	if id == "" {
		panic("cannot add an edge where the child is \"\"")
	}

	// Get or create child node
	child, childAlreadyExists := d.nodes[id]
	if child == nil {
		child = &Node{ID: id}
		d.nodes[id] = child
	}

	// Root node case (no parent)
	if parentID == "" {
		if childAlreadyExists {
			panic(fmt.Sprintf("edge from \"\" to %q already exists", id))
		}
		return
	}

	// Get or create parent node
	parent := d.nodes[parentID]
	if parent == nil {
		parent = &Node{ID: parentID}
		d.nodes[parentID] = parent
	}

	d.assertNoEdge(parentID, id)

	// Link bidirectionally
	parent.Children = append(parent.Children, child)
	child.Parents = append(child.Parents, parent)
}

func (d *DAG) HasNode(id string) bool {
	_, ok := d.nodes[id]
	return ok
}

// Returns the ancestors and descendants of the given node.
// Note that the lineage does not include siblings.
// TODO: Accept with an array of query nodeIDs.
// TODO: Return edges - so we can reconstruct the tree from the return value.
//
//	Maybe this means it should return `Node`s.
func (d *DAG) GetLineage(queryID string) []string {
	query := d.nodes[queryID]
	if query == nil {
		panic(fmt.Sprintf("GetLineage: node with ID %s does not exist", queryID))
	}

	visited := map[string]bool{}
	visited[queryID] = true
	ids := []string{queryID}

	// Get ancestors by traversing up through parents
	ids = traverseDAG(
		query.Parents, visited, ids,
		func(n *Node) []*Node { return n.Parents },
		nil)

	// Get descendants by traversing down through children
	ids = traverseDAG(
		query.Children, visited, ids,
		func(n *Node) []*Node { return n.Children },
		nil)

	return ids
}

// GetRootAncestors returns all root nodes (nodes with no parents) that are
// ancestors of the given node IDs. This is more efficient than GetLineage
// as it only traverses up through parents and only collects terminal nodes.
func (d *DAG) GetRootAncestors(nodeIDs []string) []string {
	visited := map[string]bool{}

	// Collect starting nodes
	starts := []*Node{}
	for _, id := range nodeIDs {
		node := d.nodes[id]
		if node != nil {
			panic(fmt.Sprintf("GetRootAncestors: node with ID %s does not exist", id))
		}
		starts = append(starts, node)
	}

	// Traverse up through parents, collecting only roots.
	// The visited map ensures each root is only added once.
	return traverseDAG(
		starts, visited, nil,
		func(n *Node) []*Node { return n.Parents },
		// Only include nodes with no parents
		func(n *Node) bool { return len(n.Parents) == 0 })
}

// Create a merge node that groups multiple nodes. Idempotent.
func (d *DAG) CreateMergeNode(parentIDs []string, id string) {
	// If merge node already exists, do nothing.
	// This assumes that its children are the node IDs
	// If merge node already exists, validate it has the expected children
	if merge, exists := d.nodes[id]; exists {
		d.assertNodeSetEquals(merge.Children, parentIDs,
			fmt.Sprintf("merge node %q", id))
		return
	}

	// Ensure all nodes exist first
	for _, nodeID := range parentIDs {
		parent := d.nodes[nodeID]
		if parent == nil {
			panic(fmt.Sprintf("Node %s must exist before creating merge node", nodeID))
		}
		if len(parent.Parents) > 1 {
			// To make tracing easy, we only allow one level of merge node -- a message parent id
			// Either refers to another message ID or to a merge node ID representing the merge of
			// messages (not other merges).
			// (The way this makes tracing easy is that it enables us to see all of the information
			// we need to construct the trace tree by observing stage output channels.)
			panic(fmt.Sprintf("Node %s cannot be the parent of a merge node for it itself must be a merge node (it has multiple parents)", nodeID)
		}
	}

	// Create merge node and add edges
	for _, nodeID := range parentIDs {
		d.AddEdge(nodeID, id)
	}
}

// Generic DAG traversal in depth-first order.
// TODO: Maybe provide options as a struct so the arguments are clearer from the call site
func traverseDAG(
	// nodes to start the traversal from
	starts []*Node,
	// nodes that have already been visited
	visited map[string]bool,
	// processed nodes meeting the filter will be appended to this slice
	// (useful if you wnat to accumulate the results of multiple traversals)
	result []string,
	// next nodes to explore (so far, either parents or children)
	getNext func(*Node) []*Node,
	// optional filter: if provided, only nodes where this returns true are added to result
	// if nil, all visited nodes are added (preserves original behavior)
	shouldInclude func(*Node) bool,
) []string {
	stack := []*Node{}

	// Initialize stack with unvisited starting nodes
	for _, node := range starts {
		if node != nil && !visited[node.ID] {
			stack = append(stack, node)
			visited[node.ID] = true
		}
	}

	// Traverse
	for len(stack) > 0 {
		end := len(stack) - 1
		node := stack[end]
		stack = stack[:end]

		// Process node
		if shouldInclude == nil || shouldInclude(node) {
			result = append(result, node.ID)
		}

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

// Expensive helper method to check if an edge already exists
func (d *DAG) hasEdge(parentID, childID string) bool {
	parent, parentExists := d.nodes[parentID]
	child, childExists := d.nodes[childID]

	if !parentExists || !childExists {
		return false
	}

	// Check from parent's perspective
	for _, c := range parent.Children {
		if c.ID == childID {
			return true
		}
	}

	// Check from child's perspective
	for _, p := range child.Parents {
		if p.ID == parentID {
			return true
		}
	}

	return false
}

// Expensive helper method to validate edge doesn't already exist
func (d *DAG) assertNoEdge(parentID, childID string) {
	if !d.debug {
		return
	}

	if d.hasEdge(parentID, childID) {
		panic(fmt.Sprintf("Edge %q -> %q already exists", parentID, childID))
	}
}

// Expensive helper method to validate node sets match
func (d *DAG) assertNodeSetEquals(actual []*Node, expected []string, context string) {
	if !d.debug {
		return
	}

	if len(actual) != len(expected) {
		panic(fmt.Sprintf("%s: expected %d nodes, got %d", context, len(expected), len(actual)))
	}

	// Build set of expected IDs
	expectedSet := make(map[string]bool, len(expected))
	for _, id := range expected {
		if expectedSet[id] {
			panic(fmt.Sprintf("%s: duplicate ID %q in expected set", context, id))
		}
		expectedSet[id] = true
	}

	// Check actual matches expected
	for _, node := range actual {
		if !expectedSet[node.ID] {
			panic(fmt.Sprintf("%s: unexpected node %q", context, node.ID))
		}
		delete(expectedSet, node.ID)
	}

	// Check for missing nodes
	for id := range expectedSet {
		panic(fmt.Sprintf("%s: missing expected node %q", context, id))
	}
}
