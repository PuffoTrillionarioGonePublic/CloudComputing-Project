# from dataclasses import dataclass
from typing import Callable, List, Tuple, Union
import math
from base64 import b64decode, b64encode
from bitarray import bitarray as _bitarray

class bitarray(_bitarray):
	""" Same interface as bitarray.bitarray but better api
	"""
	def __init__(self, *args, **kwargs):
		self.setall(0)	

	@classmethod
	def frombytes(cls, a: bytes, *, endian: str ="big") -> "bitarray":
		""" Create a bit array from bytes, similar to bitarray.bitarray but oneline
		"""
		b = cls(0, endian=endian)
		super().frombytes(b, a)
		return b

	@classmethod
	def fromint(cls, a: int, *, size: int = 0) -> "bitarray":
		""" Create bit array from int, bit at index 0 (lsb) will be bit at index 0 for the bitarray
		"""
		s = max(size, a.bit_length())
		r = cls.frombytes(a.to_bytes((s+7)//8, "big"))
		r.reverse()
		return r

# @dataclass
class BloomFilter:
	""" Bloom filter
	`bitvec`: bit array with `m` bits
	`m`: bit length of the `bitvec`
	`hash`: the hash function to use (with the following signature: (data: bytes, seed: int) -> int
	`seeds`: list of seeds in order to have k different hash functions
	"""
	bitvec: bitarray
	m: int
	hash: Callable[[bytes, int], int]
	seeds: List[int]

	def __init__(self, bitvec, m, hash, seeds):
		self.bitvec = bitvec
		self.m = m
		self.hash = hash
		self.seeds = seeds

	def __contains__(self, val: bytes) -> bool:
		""" Check if the bloom filter contains `val`
		"""
		for seed in self.seeds:
			i = self.hash(val, seed) % self.m
			if not self.bitvec[i]:
				return False
		return True
	
	def add(self, val: bytes):
		""" Add `val` to the bloom filter
		"""
		for seed in self.seeds:
			i = self.hash(val, seed) % self.m
			self.bitvec[i] = 1
	
	def toJson(self, **kwargs) -> dict:
		""" Return a dict that is json serializable and add eventual kwargs:
		{
		"hash_seeds": [0, 1, 2, 3], 
		"filter_lenght": 87, 
		"bitvector": "GyufZL3uGz01Gsw="
		}
		"""
		json = dict()
		json["hash_seeds"] = self.seeds
		json["filter_lenght"] = self.m
		json["bitvector"] = b64encode(self.bitvec.tobytes()).decode()
		return {**json, **kwargs}
	
	@classmethod
	def fromJson(cls, json: dict, **kwargs) -> "BloomFilter":
		""" Create instance of BloomFilter from dict and forward to constructor eventual kwargs:
		{
		"hash_seeds": [0, 1, 2, 3], 
		"filter_lenght": 87, 
		"bitvector": "GyufZL3uGz01Gsw="
		}
		"""
		bitvec = bitarray.frombytes(b64decode(json["bitvector"]))
		bf = cls(bitvec=bitvec, m=json["filter_lenght"], seeds=json["hash_seeds"], **kwargs)
		return bf
		

	@staticmethod
	def optimal_m(n: int, epsilon: float) -> int:
		""" Calulate size of the bit array `m` given the number of elements and the desired false positive probability
		"""
		m = -n*math.log(epsilon)/math.pow(math.log(2), 2) 
		return math.ceil(m)
	
	@staticmethod
	def optimal_k(n: int, epsilon: float) -> int:
		""" Calulate number of hash functions `k` given the number of elements and the desired false positive probability
		"""
		k = -math.log2(epsilon)
		return math.ceil(k)