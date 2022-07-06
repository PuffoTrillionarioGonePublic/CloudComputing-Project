def murmurhash2(data: bytes, seed: int) -> int:
	""" Implementation of the 32 bit murmurhash version 2 function
	"""
	MASK = ( 1<<32 ) - 1
	m = 0x5bd1e995

	h = seed ^ len(data)

	len_4 = len(data) >> 2

	for i in range(len_4):
		k = int.from_bytes(data[i*4: i*4 + 4], "little")

		k = ( k * m ) & MASK
		k ^= k >> 24
		k = ( k * m ) & MASK
		h = (h * m) & MASK
		h ^= k
	
	len_m = len_4 << 2
	left = len(data) - len_m

	if left >= 3:
		h ^= (data[-3] << 16) & MASK
	if left >= 2:
		h ^= (data[-2] << 8) & MASK
	if left >= 1:
		h ^= data[-1]
		h = (h * m) & MASK
	
	h ^= h >> 13
	h = (h * m) & MASK
	h ^= h >> 15

	# return a signed 32 bit number like in the java version
	if h & (1<<31):
		h -= (1<<32)
	return h