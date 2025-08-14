package lineage

import "fmt"

type Node struct {
	ID       string
	Parents  []*Node
	Children []*Node
}

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
		panic("cannot add an edge where the child is a root node (id = \"\")")
	}

	// Get or create child node
	child, childAlreadyExists := d.nodes[id]
	if child == nil {
		child = &Node{ID: id}
		d.nodes[id] = child
	}

	// Root node case (no parent)
	if parentID == "" {
		if d.debug {
			if childAlreadyExists {
				panic(fmt.Sprintf("edge from \"\" to %q already exists", id))
			}
		}
		return
	}

	// Get or create parent node.
	parent := d.nodes[parentID]
	if parent == nil {
		parent = &Node{ID: parentID}
		d.nodes[parentID] = parent
	}

	if d.debug {
		// Check if edge already exists to avoid duplicates
		for _, p := range child.Parents {
			if p.ID == parentID {
				panic(fmt.Sprintf("Edge %q -> %q already exists child's parents", parentID, id))
			}
		}

		for _, c := range parent.Children {
			if c.ID == id {
				panic(fmt.Sprintf("Edge %q -> %q already exists in parent's children", parentID, id))
			}
		}
	}

	// Link bidirectionally
	parent.Children = append(parent.Children, child)
	child.Parents = append(child.Parents, parent)
}

func (d *DAG) HasNode(id string) bool {
	_, ok := d.nodes[id]
	return ok
}

// Returns the ancestors and descendants of the given node.
func (d *DAG) GetLineage(queryID string) []string {
	query := d.nodes[queryID]
	if query == nil {
		return nil
	}

	visited := map[string]bool{}
	visited[queryID] = true
	ids := []string{queryID}

	// Get ancestors by traversing up through parents
	ids = traverseDAG(query.Parents, visited, ids, func(n *Node) []*Node {
		return n.Parents
	})

	// Get descendants by traversing down through children
	ids = traverseDAG(query.Children, visited, ids, func(n *Node) []*Node {
		return n.Children
	})

	return ids
}

// Create a merge node that groups multiple nodes. Idempotent.
func (d *DAG) CreateMergeNode(mergeID string, nodeIDs []string) {
	// If merge node already exists, do nothing.
	// This assumes that its children are the node IDs
	if merge, ok := d.nodes[mergeID]; ok {
		_ = merge
		if len(merge.Children) != len(nodeIDs) {
			panic("merge node already exists with unexpected number of children")
		}

		if d.debug {
			// Check that the merge node has exactly the expected children
			ids := map[string]bool{}
			for _, id := range nodeIDs {
				ids[id] = true
			}
			if len(nodeIDs) != len(ids) {
				panic("nodeIDs contains duplicate nodes")
			}

			for _, id := range nodeIDs {
				if _, ok := ids[id]; !ok {
					panic(fmt.Sprintf("merge node exists with incorrect children: node %q is in nodeIDs but not the existing merge node.", id))
				}
			}
		}

		return
	}

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

// Generic DAG traversal in depth-first order.
func traverseDAG(
	// nodes to start the traversal from
	starts []*Node,
	// nodes that have already been visited
	visited map[string]bool,
	// processed nodes will be appended to this slice
	// (useful if you wnat to accumulate the results of multiple traversals)
	result []string,
	// next nodes to explore (so far, either parents or children)
	getNext func(*Node) []*Node,
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
