"""
Consistent Hashing Ring with Virtual Nodes.
Provides deterministic, balanced replica placement with minimal key migration on topology change.
"""
import bisect
import hashlib
from typing import List, Dict, Set, Optional
import config

def _hash_str(val: str) -> int:
    """Compute 64-bit integer hash using SHA-256."""
    digest = hashlib.sha256(val.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False)

class ConsistentHashRing:
    def __init__(self, virtual_nodes_count: int = config.VIRTUAL_NODES_PER_PHYSICAL):
        self.virtual_nodes_count = virtual_nodes_count
        self.ring: List[int] = []  # Sorted list of hash values
        self.vnode_to_node: Dict[int, str] = {}  # Map hash -> physical node_id
        self.nodes: Set[str] = set()

    def add_node(self, node_id: str) -> None:
        """Add a physical node and its virtual replicas to the hash ring."""
        if node_id in self.nodes:
            return
        self.nodes.add(node_id)
        for i in range(self.virtual_nodes_count):
            vnode_key = f"{node_id}#vnode_{i}"
            vhash = _hash_str(vnode_key)
            self.vnode_to_node[vhash] = node_id
            bisect.insort(self.ring, vhash)

    def remove_node(self, node_id: str) -> None:
        """Remove a physical node and all its virtual replicas from the hash ring."""
        if node_id not in self.nodes:
            return
        self.nodes.remove(node_id)
        hashes_to_remove = set()
        for i in range(self.virtual_nodes_count):
            vnode_key = f"{node_id}#vnode_{i}"
            vhash = _hash_str(vnode_key)
            hashes_to_remove.add(vhash)
            self.vnode_to_node.pop(vhash, None)

        self.ring = [h for h in self.ring if h not in hashes_to_remove]

    def get_nodes_for_key(
        self,
        key: str,
        count: int,
        exclude_nodes: Optional[Set[str]] = None
    ) -> List[str]:
        """
        Clockwise walk from key hash to select 'count' distinct physical nodes.
        Filters out any nodes in 'exclude_nodes'.
        """
        if not self.ring or not self.nodes:
            return []

        exclude = exclude_nodes or set()
        available_nodes = [n for n in self.nodes if n not in exclude]
        if not available_nodes:
            return []

        target_count = min(count, len(available_nodes))
        key_hash = _hash_str(key)

        # Binary search for clockwise starting position
        idx = bisect.bisect_right(self.ring, key_hash)
        total_ring_size = len(self.ring)

        selected_nodes: List[str] = []
        seen_nodes: Set[str] = set()

        for step in range(total_ring_size):
            ring_index = (idx + step) % total_ring_size
            node_id = self.vnode_to_node[self.ring[ring_index]]
            if node_id not in seen_nodes and node_id not in exclude:
                seen_nodes.add(node_id)
                selected_nodes.append(node_id)
                if len(selected_nodes) == target_count:
                    break

        return selected_nodes

    def analyze_remapping(self, sample_keys: List[str], new_node_id: str) -> Dict[str, float]:
        """
        Test utility: compute fraction of keys remapped after adding a new node.
        Theoretical expectation for N -> N+1 nodes is ~ 1 / (N+1).
        """
        before_mapping = {k: self.get_nodes_for_key(k, 1)[0] for k in sample_keys if self.nodes}
        self.add_node(new_node_id)
        after_mapping = {k: self.get_nodes_for_key(k, 1)[0] for k in sample_keys}
        
        remapped_count = sum(1 for k in sample_keys if before_mapping.get(k) != after_mapping.get(k))
        fraction_remapped = remapped_count / max(len(sample_keys), 1)

        # Cleanup
        self.remove_node(new_node_id)
        return {
            "total_keys": len(sample_keys),
            "remapped_keys": remapped_count,
            "fraction_remapped": round(fraction_remapped, 4),
            "expected_fraction": round(1.0 / (len(self.nodes) + 1), 4) if self.nodes else 1.0
        }
