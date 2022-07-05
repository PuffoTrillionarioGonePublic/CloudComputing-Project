package it.puffotrillionario.hadoop;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
// JSON - https://code.google.com/archive/p/json-simple/
// Tutorial: https://www.geeksforgeeks.org/parse-json-java/
import org.json.simple.parser.*;

/**
 * Hello world!
 */
public class App extends Configured implements Tool
{
    // Filename marking job completion
    static final String _SUCCESS = "_SUCCESS";
    // property containing the name of the folder containing
    // bloom filters as JSON
    static final String BLOOM_FOLDER_PROPERTY = "bloom-filters-folder";
    // property containing the names of the BF files 
    static final String BLOOM_FILTER_FILES_PROPERTY = "bloom-filters-files";
    // match files containing indexes of all bloom filters
    private static String glob_matching_indexes = "bloom_filter_*-r-*";

    // used to represent a bloom filter to 
    private static class BloomFilter {
        public static String HASH_FUNCTION_MARKER = "mmh2";
        private int rating;
        private int N;
        private BitSet bs;
        private int[] seeds;
        // hash function
        private Hash h = MurmurHash.getInstance();

        public int getRating() {
            return rating;
        }

        // read a file containing an
        public static BloomFilter parse(BufferedReader r) throws IOException, ParseException {
            int rating;
            int N;
            BitSet bs;
            int[] seeds;
            /**
            rating:         int
            hash_function:  string
            filter_lenght:  int
            hash_seeds:     int[]
            bitvector:      string
             */
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject)parser.parse(r);
            String hashType = (String)json.get("hash_function");
            if (!hashType.equals(HASH_FUNCTION_MARKER)) {
                throw new IOException("Bad json, expected hash_function: " + HASH_FUNCTION_MARKER + ", found " + hashType);
            }
            rating = (int)((Long)json.get("rating")).longValue();
            N = (int)((Long)json.get("filter_lenght")).longValue();
            JSONArray h_seeds = (JSONArray)json.get("hash_seeds");
            seeds = new int[h_seeds.size()];
            for (int i=0; i!=seeds.length; ++i) {
                seeds[i] = (int)((Long)h_seeds.get(i)).longValue();
            }
            Base64.Decoder dec = Base64.getDecoder();
            bs = BitSet.valueOf(dec.decode((String)json.get("bitvector")));

            return new BloomFilter(rating, N, seeds, bs);
        }

        public BloomFilter(int rating, int N, int[] seeds, BitSet bs) {
            this.rating = rating;
            this.N = N;
            this.bs = new BitSet(N);
            this.seeds = seeds;
            this.bs = bs;
        }

        private int hash(byte[] bytes, int seed) {
            return (int)((((long)h.hash(bytes, seed)%N)+N)%N);
        }

        // test key in bloom filter
        public boolean test(byte[] key) {
            for (int seed : seeds) {
                if (!bs.get(hash(key, seed))) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class TestArgs {
        public static int REQUIRED_ARGS_LENGHT = 3;
        public String filters;
        public String join;
        public String results;
    }

    private static TestArgs parse(String[] args) {
        TestArgs ans = new TestArgs();
        if (args.length != TestArgs.REQUIRED_ARGS_LENGHT) {
            System.err.println("Expected " + TestArgs.REQUIRED_ARGS_LENGHT + " arguments, found " + args.length);
            return null;
        }
        ans.filters = args[0];
        ans.join = args[1];
        ans.results = args[2];
        return ans;
    }

    private static void help() {
        System.err.println("Calculate false positives rate on all bloom filters");
        System.err.println("Usage: hadoop-text <filters> <join> <results>");
        System.err.println("\t<filters>:         path to folder containing file(s) of bloom filters");
        System.err.println("\t<join>:            path to folder containing tuples (id, title, vote)");
        System.err.println("\t<results>:         path to folder to store results in");
    }
    
    static void printArgs(TestArgs args) {
        System.out.println("Arguments:");
        System.err.println("\t<filters>:         " + args.filters);
        System.err.println("\t<join>:            " + args.join);
        System.err.println("\t<results>:         " + args.results);
    }

    public static void main( String[] args ) throws Exception
    {
        int exitCode = ToolRunner.run(new App(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        TestArgs parsedArgs = parse(args);
        if (parsedArgs == null) {
            help();
            return -1;
        }
        printArgs(parsedArgs);
        // access fs to check if results already exists
        FileSystem hdfs = FileSystem.get(getConf());
        boolean results_exists = hdfs.exists(new Path(parsedArgs.results, _SUCCESS));
        if (results_exists) {
            System.out.println("Result folder already exists, skipping MapReduce execution");
        } else {
            System.out.println("Launching Test job!");
            if (!launchTestJob(parsedArgs)) {
                System.err.println("Test job failed!");
                return -1;
            }
            System.out.println("Test Job completed!");
        }

        return 0;
    }

    // first: count tested items
    // second: count false positive items
    public static class IntPair implements Writable {
        private int first;
        private int second;

        // accumulator
        public IntPair inc(IntPair other) {
            this.first += other.first;
            this.second += other.second;
            return this;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        public IntPair setFirst(int first) {
            this.first = first;
            return this;
        }

        public IntPair setSecond(int second) {
            this.second = second;
            return this;
        }

        public IntPair set(int first, int second) {
            return setFirst(first).setSecond(second);
        }

        public IntPair() {}
        public IntPair(int first, int second) {
            this.first = first;
            this.second = second;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(first);
            out.writeInt(second);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            first = in.readInt();
            second = in.readInt();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof IntPair) {
                IntPair other = (IntPair)obj;
                return first == other.first && second == other.second;
            }
            return false;
        }

        // Seggested here:
        //  https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/Writable.html#write-java.io.DataOutput-
        public static IntPair read(DataInput in) throws IOException {
            IntPair w = new IntPair();
            w.readFields(in);
            return w;
        }

        @Override
        public String toString() {
            return first + "\t" + second + "\t" + (float)second/first;
        }
    }

    // Read tuple (id, title, vote)
    // Assert tuple is in the right filter (i.e. same filter of its vote)
    // 
    public static class TestMapper extends Mapper<LongWritable, Text, IntWritable, IntPair> {
        // how many fields in each line of the join output
        private static int FIELDS_PER_LINE = 3;

        private IntWritable outk = new IntWritable();
        private IntPair outv = new IntPair();

        // associate each vote with the corresponding BF
        private Map<Integer, BloomFilter> voteBF;
        
        // read bloom filters from files to perform tests
        @Override
        protected void setup(Mapper<LongWritable, Text, IntWritable, IntPair>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            // read files containing bloom filters from the specified folders
            Configuration conf = context.getConfiguration();
            // read propery containing BF file names
            List<String> bf_file_names = Arrays.asList(conf.get(BLOOM_FILTER_FILES_PROPERTY).split(","));
            // get FS object to retrieve files
            FileSystem hdfs = FileSystem.get(conf);
            // get URIs to BF files
            URI[] bf_cache_uris = context.getCacheFiles();
            // prepare map to maintain filters
            voteBF = new TreeMap<>();
            // one by one, read files and parse filters
            for (URI bf_uri : bf_cache_uris) {
                Path path = new Path(bf_uri);
                // if the file contain a bloom filter
                if (bf_file_names.contains(path.getName())) {
                    try (FSDataInputStream ins = hdfs.open(path);
                        BufferedReader r = new BufferedReader(new InputStreamReader(ins));
                    ) {
                        BloomFilter bf = BloomFilter.parse(r);
                        voteBF.put(bf.getRating(), bf);
                    } catch (ParseException e) {
                        throw new RuntimeException("Error in parsing Json: " + path, e);
                    }
                }
            }
        }

        // check imput line w.r.t. all rows in the dataset
        // to check false positives
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntPair>.Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if (line.length != FIELDS_PER_LINE) {
                return;
            }
            String id = line[0];
            byte[] id_bytes = id.toLowerCase().getBytes(StandardCharsets.UTF_8);
            int rVote;
            try {
                rVote = Math.round(Float.parseFloat(line[2]));
            } catch (Exception e) {
                return;
            }
            // assert item is in the right filter
            if (!voteBF.get(rVote).test(id_bytes)) {
                throw new RuntimeException("Id '" + id + "' not found in filter " + rVote);
            }
            // test line w.r.t. all filters
            for (Map.Entry<Integer,BloomFilter> c : voteBF.entrySet()) {
                // do not check for its own filter
                if (c.getKey() != rVote) {
                    // check for id in bloom filter
                    boolean false_positive = c.getValue().test(id_bytes);
                    // key is filter vote
                    outk.set(c.getKey());
                    // value.first is 1 for one test,
                    // value.second is 1 for false positive, otherwise 0
                    outv.set(1, false_positive?1:0);
                    context.write(outk, outv);
                }
            }
        }
    }

    public static class TestReducer extends Reducer<IntWritable, IntPair, IntWritable, IntPair> {
        private IntPair outv;
        @Override
        protected void reduce(IntWritable key, Iterable<IntPair> values,
                Reducer<IntWritable, IntPair, IntWritable, IntPair>.Context context)
                throws IOException, InterruptedException {
            outv = new IntPair();
            for (IntPair v : values) {
                outv.inc(v);
            }
            context.write(key, outv);
        }
    }

    private boolean launchTestJob(TestArgs args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = getConf();
        // save name of the folder storing the filters
        conf.set(BLOOM_FOLDER_PROPERTY, args.filters);

        // access FS to obtain names of the bloom filter files
        FileSystem hdfs = FileSystem.get(getConf());
        //FileStatus[] files = hdfs.globStatus(new Path(args.filters, glob_matching_indexes));
        FileStatus[] files = hdfs.globStatus(new Path(args.filters, glob_matching_indexes));
        // URIs of the files to be localised using tha cache API
        URI[] bf_uris;
        if (files.length != 10) {
            System.err.println("BF files not found! Found:");
            for (FileStatus fs : files) {
                System.err.println("\t" + fs.getPath());
            }
            throw new RuntimeException("Expected 10 bloom filters files, found " + files.length);
        } else {
            System.out.println("Configuring distributed cache for BF files");
            // allocate array of URIs to be used later
            bf_uris = new URI[files.length];
            // to store file names
            StringBuilder str_builder = new StringBuilder(512);
            for (int i=0; i!=files.length; ++i) {
                if (str_builder.length() > 0) {
                    str_builder.append(",");
                }
                Path path = files[i].getPath();
                URI uri = path.toUri();
                bf_uris[i] = uri;
                // list files containing bloom filters
                str_builder.append(path.getName());
                System.out.println("\tpath:" + path + "\turi:" + uri);
            }
            String prop = str_builder.toString();
            System.out.println("Set '" + BLOOM_FILTER_FILES_PROPERTY + "' to '" + prop + "'");
            // add to conf property
            conf.set(BLOOM_FILTER_FILES_PROPERTY, prop);
        }
        // generate Job file
        Job job = Job.getInstance(conf, "cloud-project-test");

        // add BF files to the distributed cache
        job.setCacheFiles(bf_uris);

        job.setJarByClass(App.class);
        job.setMapperClass(TestMapper.class);
        job.setCombinerClass(TestReducer.class);
        job.setReducerClass(TestReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntPair.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntPair.class);

        job.setInputFormatClass(TextInputFormat.class);

        // add input/output paths
        FileInputFormat.addInputPath(job, new Path(args.join));
        FileOutputFormat.setOutputPath(job, new Path(args.results));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        return job.waitForCompletion(true);
    }
}
