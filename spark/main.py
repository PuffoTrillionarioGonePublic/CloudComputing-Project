import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ByteType
from pyspark.sql.functions import round as pyspark_round, col as pyspark_col
from typing import Dict, Tuple, List
import json 
from util.bloomfilter import BloomFilter, bitarray
import mmh3
import argparse

# Create spark global objects
sc = SparkContext(master="yarn", appName="bloomfilter")
spark = SparkSession(sc)

def load_dataset(*, ratings_file: str, titles_file: str) -> pyspark.rdd.RDD:
	""" Load dataset and return a rdd in the form `(rating, title)`
	"""

	# load ratings from file 
	ratings_schema = StructType([StructField("tconst", StringType(), False), StructField("averageRating", FloatType(), False), StructField("numVotes", IntegerType(), False)])
	df_ratings = spark.read.csv(ratings_file, sep='\t', schema=ratings_schema, header=True)\
		.withColumn('averageRating', pyspark_round(pyspark_col("averageRating")).cast(ByteType()))\
		.withColumnRenamed("averageRating", 'roundedRating')\
		.select('roundedRating', 'tconst')

	"""
	# load titles from file 
	df_titles = spark.read.csv(titles_file, sep='\t', header=True)\
		.select('tconst', 'originalTitle')

	# join the two dataframes and convert to rdd 
	dataset = df_ratings.join(df_titles, on='tconst')\
		.select('roundedRating', 'originalTitle')\
		.rdd
	"""

	return df_ratings.rdd

def build_bloomfilter(dataset: pyspark.rdd.RDD, *, epsilon=0.1) -> Dict[int, BloomFilter]:
	""" Given the dataset in the form `(rating, title)` build one bloomfilter for each rating and return a dictionary mapping rating -> bloomfilter
	"""

	# count number of movies per rating 
	num_movies_per_rating = dataset.countByKey()
	print(num_movies_per_rating)

	# function that takes a pair (rating, title) and returns a pair (rating, int with the bits set to one at the positions specified by the hash function)
	def setbits(x: Tuple[int, str]) -> Tuple[int, int]:
		rating, title = x
		m = BloomFilter.optimal_m(num_movies_per_rating[rating], epsilon)
		k = BloomFilter.optimal_k(num_movies_per_rating[rating], epsilon)
		rv = 0
		for seed in range(k):
			i = mmh3.hash(title.lower().encode("utf-8"), seed) % m
			rv |= 1<<i
		return rating, rv

	# calculate bloom filter
	bf_tmp = dataset.map(setbits)\
		.reduceByKey(int.__or__)\
		.collect()

	# wrap in bloom filter class and return
	bfs = dict()
	for rating, ibitvec in bf_tmp:
		m = BloomFilter.optimal_m(num_movies_per_rating[rating], epsilon)
		k = BloomFilter.optimal_k(num_movies_per_rating[rating], epsilon)
		bitvec = bitarray.fromint(ibitvec, size=m)
		bfs[rating] = BloomFilter(bitvec=bitvec, m=m, hash=mmh3.hash, seeds=list(range(k)))
	
	return bfs

def test_bloomfilter(bf: BloomFilter, rating: int, testset: List[Tuple[int, str]]) -> Tuple[int, int]:
	""" Test false positive rate of the BloomFilter `bf` (whose movies have rating `rating`)
	"""
	total, falsep = 0, 0
	for t_rating, t_title in testset:
		if t_rating == rating:
			assert t_title.lower().encode("utf-8") in bf
			continue
		if t_title.lower().encode("utf-8") in bf:
			falsep += 1
		total += 1
	return falsep, total


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('hdfs_input_dir', type=str, help='Path of input directory')
	parser.add_argument('local_output_dir', type=str, help='Path of output directory')
	parser.add_argument('p', type=float, help='False positive probability')
	parser.add_argument('--no-calculate', action='store_true', help='Dont calculate bloom filters, load them from file')
	parser.add_argument('--no-test', action='store_true', help='Dont test false positive probability')
	args = parser.parse_args()
	print("Running with args: {args}")


	dataset = load_dataset(
		ratings_file=f"{args.hdfs_input_dir}/title.ratings.tsv", 
		titles_file=f"{args.hdfs_input_dir}/title.basics.tsv")
	bfs: Dict[int, BloomFilter]

	# calculate bloom filters
	if not args.no_calculate:
		bfs = build_bloomfilter(dataset, epsilon=args.p)
	
		for k,v in bfs.items():
			with open(f"{args.local_output_dir}/bf-{k}.json", "w") as f:
				json.dump(v.toJson(rating=k, hash_function="mmh2"), f)
	# load bloom filter from file
	else:
		bfs = dict()
		for rating in range(1, 11):
			with open(f"{args.local_output_dir}/bf-{rating}.json", "r") as f:
				bfs[rating] = BloomFilter.fromJson(json.load(f), hash=mmh3.hash)
	# test false positive probability
	if not args.no_test:
		testset = dataset.sample(withReplacement=False, fraction=0.05).collect()
		for rating in range(1, 11):
			falsep, total = test_bloomfilter(bfs[rating], rating, testset=testset)
			print(f"{rating:>2}: {falsep:>4}/{total:>4} = {falsep/total:.4f}")

if __name__ == "__main__":
	main()