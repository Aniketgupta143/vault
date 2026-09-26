"""
Erasure Coding engine using Reed-Solomon (k+m).
Enables Vault to reconstruct objects with ANY k of k+m shards,
surviving up to m node failures at only (k+m)/k overhead (1.5x for 4+2 vs 3.0x for RF=3).
"""
import reedsolo
from typing import List, Dict, Tuple, Optional
import config

class ErasureCoder:
    def __init__(self, k: int = config.EC_DATA_SHARDS, m: int = config.EC_PARITY_SHARDS):
        self.k = k
        self.m = m
        self.rs = reedsolo.RSCodec(m)

    def encode(self, data: bytes) -> Tuple[List[bytes], int]:
        """
        Split byte payload into k data shards and generate m parity shards.
        Returns (list_of_k_plus_m_shards, original_length).
        """
        orig_len = len(data)
        if orig_len == 0:
            return [b"" for _ in range(self.k + self.m)], 0

        # Pad payload to multiple of k
        pad_len = (self.k - (orig_len % self.k)) % self.k
        padded = data + (b"\x00" * pad_len)
        shard_len = len(padded) // self.k

        data_shards = [bytearray(padded[i * shard_len : (i + 1) * shard_len]) for i in range(self.k)]
        parity_shards = [bytearray(shard_len) for _ in range(self.m)]

        # Column-by-column Reed-Solomon systematic encoding
        for col in range(shard_len):
            col_bytes = bytes([data_shards[row][col] for row in range(self.k)])
            encoded_col = self.rs.encode(col_bytes)
            for p in range(self.m):
                parity_shards[p][col] = encoded_col[self.k + p]

        all_shards = [bytes(s) for s in data_shards] + [bytes(s) for s in parity_shards]
        return all_shards, orig_len

    def decode(self, available_shards: Dict[int, bytes], original_len: int) -> bytes:
        """
        Reconstruct the original payload from ANY k of k+m shards.
        'available_shards' maps shard_index (0..k+m-1) to shard bytes.
        """
        if original_len == 0:
            return b""

        if len(available_shards) < self.k:
            raise RuntimeError(
                f"Insufficient shards for reconstruction: got {len(available_shards)}, required k={self.k}"
            )

        sample_shard = next(iter(available_shards.values()))
        shard_len = len(sample_shard)

        erasures = [i for i in range(self.k + self.m) if i not in available_shards]
        reconstructed_data = [bytearray(shard_len) for _ in range(self.k)]

        for col in range(shard_len):
            col_vec = bytearray(self.k + self.m)
            for idx in range(self.k + self.m):
                if idx in available_shards:
                    col_vec[idx] = available_shards[idx][col]
                else:
                    col_vec[idx] = 0

            decoded_col, _, _ = self.rs.decode(bytes(col_vec), erase_pos=erasures)
            for row in range(self.k):
                reconstructed_data[row][col] = decoded_col[row]

        reconstructed = b"".join(reconstructed_data)[:original_len]
        return reconstructed

global_ec_coder = ErasureCoder()
