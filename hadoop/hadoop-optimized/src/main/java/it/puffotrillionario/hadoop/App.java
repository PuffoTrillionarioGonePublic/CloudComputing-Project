package it.puffotrillionario.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.util.Base64;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;

/**
 * Hello world!
 */
public class App extends Configured implements Tool
{
    static final String HASH_SEEDS_PROPERTY = "hash_keys";
    //static final String BLOOM_SIZE_PROPERTY = "bloom_size";
    static final String MOVIE_COUNT_PROPERTY = "movie_count";
    // number of hash functions to be used
    //static final String N_HASH_FUNCTION_PROPERTY = "hash_count";
    // one bloom filter for each vote
    static final String DISTINCT_BLOOM_SIZE_PROPERTY = "distinct_bloom_size";
    static final String EXPECTED_FALSE_POSITIVE_RATE = "expected_false_positive_rate";
    static final int DEFAULT_BLOOM_SIZE = 4096*8; // Fit exactly one page in memory
    static final String ratings = "R";  // title.ratings.tsv
    static final String basics = "B";   // title.basics.tsv

    // counter used to store 
    static final String movie_counter = "movie-by-rvote";
    // name used by hadoop by default for the first output file
    // produced by a reducer
    static final String part_r_00000 = "part-r-00000";
    static final String _SUCCESS = "_SUCCESS";

    // name of the file containing the keys used by
    static final String bf_h_seeds = "hash-seeds";
    // name of the file containing the false positive rate expected with the bloom filters
    static final String expected_fp_rate = "expected-false-positive-rate";
    // name of the file containing the sizes of bloom filters
    static final String bloom_filter_sizes = "bloom-filter-sizes";

    // const improves perfomances
    private static final double log_2 = Math.log(2.0);
    private static final double log_2_squared = log_2*log_2;
    // Formula extracted from assignment:
    //  m=-n*ln(p)/ln2(2)
    private static int calculateBFSize(int n, double p) {
        return (int)Math.ceil(-n*Math.log(p)/log_2_squared);
    }

    // calculate number of hash functions to use
    private static int calculateK(double p) {
        return (int)Math.ceil(-Math.log(p)/log_2);
    }

    // Join side on Ratings
    public static class JoinRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outk = new Text();
        private Text outv = new Text();
        private static final int RATING_COLUMNS_COUNT = 3;
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // get text
            String[] line = value.toString().split("\t");
            // check line length and header
            if (line.length != RATING_COLUMNS_COUNT || line[0].equals("tconst")) {
                return;
            }
            // check numeric vote
            try {
                Float.parseFloat(line[1]);
            } catch(Exception e) {
                return;
            }
            outk.set(line[0]);
            outv.set(ratings + "\t" + line[1]);
            context.write(outk, outv);
        }
    }

    // Join side on Titles
    public static class JoinTitleMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outk = new Text();
        private Text outv = new Text();
        private static final int TITLES_COLUMNS_COUNT = 9;
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            // get text
            String[] line = value.toString().split("\t");
            // check line length and header
            if (line.length != TITLES_COLUMNS_COUNT || line[0].equals("tconst")) {
                return;
            }
            outk.set(line[0]);
            outv.set(basics + "\t" + line[3]);
            context.write(outk, outv);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        class Res {
            public String title;
            public String vote;

            @Override
            public String toString() {
                return this.title + "\t" + this.vote;
            }
        }

        //private IntWritable outv = new IntWritable();
        private Text outv = new Text();
        // Use Hadoop counters to count movies by rounded average vote
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            Res ans = new Res();
            int counter = 0;
            boolean vote = false, title = false;
            for (Text v : values) {
                if (counter++ == 2) {
                    //throw new RuntimeException("Inconsistent input found in reducer!");
                    return;
                }
                String str = v.toString();
                String[] split = str.split("\t");
                switch (split[0]) {
                    case basics:
                        if (title == true) {
                            //throw new RuntimeException("Reducer: double title!");
                            return;
                        }
                        title = true;
                        ans.title = split[1];
                        break;
                    case ratings:
                        if (vote == true) {
                            //throw new RuntimeException("Reducer: double title!");
                            return;
                        }
                        vote = true;
                        ans.vote = split[1];
                        break;
                }
            }
            if (counter == 2) {
                outv.set(ans.toString());
                context.write(key, outv);
                context.getCounter(movie_counter, Integer.toString(Math.round(Float.parseFloat(ans.vote)))).increment(1);
            }
        }
    }

    public static void main( String[] args ) throws Exception
    {
        int exitCode = ToolRunner.run(new App(), args);
        System.exit(exitCode);
    }

    // Array [1,2,3] become String "1,2,3"
    private static String intArrayToString(int[] array) {
        String[] s = new String[array.length];
        for (int i=0; i!=array.length; ++i) {
            s[i] = Integer.toString(array[i]);
        }
        return String.join(",", s);
    }

    private static class JobArgs {
        public static int REQUIRED_ARGS_LENGHT = 7;
        // Paths to I/O
        public String in_titles, in_ratings, merge, count, out;
        // expected false positive rate
        public double p;
        // numbers of lines read by mappers
        public int nlines;
        // seeds used by the hash functions: 1 seed = 1 hash function
        public List<Integer> seeds;

        // Given a list of integers (es.: [1,2,3]), generate a
        // string representing its content (es.: "1,2,3")
        public String getSeedsString() {
            return String
                .join(",",
                    seeds.stream().map(i -> i.toString()).toArray(String[]::new)
                );
        }
    }

    private static JobArgs parse(String[] args) {
        if (args.length < JobArgs.REQUIRED_ARGS_LENGHT) {
            return null;
        }
        JobArgs ans = new JobArgs();
        // path to files
        ans.in_titles  = args[0];
        ans.in_ratings = args[1];
        ans.merge = args[2];
        ans.count = args[3];
        ans.out = args[4];
        try {
            ans.p = Double.parseDouble(args[5]);
            if (ans.p <= 0.0 || ans.p >= 1.0) {
                throw new Exception();    
            }
        } catch (Exception e) {
            System.err.println("Error: <false p rate> must be a real number between 0 and 1, found '" + args[5] + "'");
            return null;
        }
        try {
            ans.nlines = Integer.parseInt(args[6]);
            // number of lines to be processed by each mapper
            if (ans.nlines <= 0) {
                throw new Exception();
            }
        } catch (Exception e) {
            System.err.println("Error: <nlines> must be a positive integer, found '" + args[6] + "'");
            return null;
        }
        // handle filter keys
        // hash keys are now randomly generated, their number is based
        // on function
        {
            // number of hash functions to be used
            final int K = calculateK(ans.p);
            final int remaining = args.length - JobArgs.REQUIRED_ARGS_LENGHT;
            List<Integer> seeds = new ArrayList<>();
            if (remaining == 0) {
                // keys are randomly generated
                Random r_engine = new Random();
                while (seeds.size() < K) {
                    int candidate = r_engine.nextInt();
                    // discard duplicates
                    if (!seeds.contains(candidate)) {
                        seeds.add(candidate);
                    }
                }
            } else if (K == remaining) {
                // keys are read from cli arguments
                for (int i=0; i!=K; ++i) {
                    seeds.add(Integer.parseInt(args[JobArgs.REQUIRED_ARGS_LENGHT + i]));
                }
            } else {
                // incoerent parameters
                System.err.println("False positive rate " + ans.p + " require " + K + " hash seeds, provided: " + remaining);
                return null;
            }
            ans.seeds = seeds;
        }
        return ans;
    }

    private static void help() {
        System.err.println("Usage: cloud-project <in-titles> <in-ratings> <tmp> <out> <filters> <bloom-size> [ k1 [ k2 ... ] ]");
        System.err.println("\t<in-titles>:       path to input file(s) containing movie titles");
        System.err.println("\t<in-ratings>:      path to input file(s) containing movie votes");
        System.err.println("\t<merge>:           path to store intermediate results");
        System.err.println("\t<count>:           path to store film count");
        System.err.println("\t<out>:             path to store final bloom keys");
        System.err.println("\t<false p rate>:    expected false positive rate, must stay between (0,1)");
        System.err.println("\t<nlines>:          number of lines to be passed to each mapper");
        System.err.println("\t[ k1 [ k2 ... ] ]: integers keys to be used to seed hash functions, must be coherent with false positive rate");
    }

    static void printArgs(JobArgs args) {
        System.out.println("Arguments:");
        System.out.println("\t<in-titles>:    " + args.in_titles);
        System.out.println("\t<in-ratings>:   " + args.in_ratings);
        System.out.println("\t<merge>:        " + args.merge);
        System.out.println("\t<count>:        " + args.count);
        System.out.println("\t<out>:          " + args.out);
        System.out.println("\t<false p rate>: " + args.p);
        System.out.println("\t<nlines>:       " + args.nlines);
        System.out.println("\t<seeds>:         " + args.getSeedsString());
    }

    // read results of second job, that is the number of movies
    // for each rounded vote (thus Bloom Filter)
    private Map<Integer,Integer> readMovieCount(JobArgs args) throws IOException {
        FileSystem hdfs = FileSystem.get(getConf());
        Map<Integer,Integer> ans = new TreeMap<>();
        // only a single file is produced from the count phase
        // Pay attention to deprecation
        //  https://docs.oracle.com/javase/8/docs/api/java/io/DataInputStream.html?is-external=true#readLine--
        try (FSDataInputStream ins = hdfs.open(new Path(args.count, part_r_00000));
            BufferedReader d = new BufferedReader(new InputStreamReader(ins));
            Stream<String> stream = d.lines();
        ) {
            String[] lines = stream.toArray(String[]::new);
            for (String line : lines) {
                String[] split = line.split("\t");
                ans.put(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
            }
        }
        return ans;
    } 

    private static void printMapII(String msg, Map<Integer,Integer> count) {
        if (msg != null) {
            System.out.println(msg);
        }
        for (Map.Entry<Integer,Integer> c : count.entrySet()) {
            System.out.println("\t" + c.getKey() + ":\t" + c.getValue());
        }
    }

    private static void printMovieCount(Map<Integer,Integer> count) {
        printMapII("Print movie count:", count);
    }

    private static Map<Integer,Integer> estimateBFsizes(double p, Map<Integer,Integer> count) {
        Map<Integer,Integer> ans = new TreeMap<>();
        for (Map.Entry<Integer,Integer> c : count.entrySet()) {
            ans.put(c.getKey(), calculateBFSize(c.getValue(), p));
        }
        return ans;
    }

    // key value in the map are put in consecutive positions,
    // even index: rounded vote
    // odd index:  movie count
    // Ex:
    // {
    //  12 => 1,
    //  25 => 100
    // }
    // become:
    //  [12,1,25,100]
    private static int[] getPairsFromMapII(Map<Integer,Integer> count) {
        int[] ans = new int[count.size()*2];
        int i = 0;
        for (Map.Entry<Integer,Integer> c : count.entrySet()) {
            ans[i++] = c.getKey();
            ans[i++] = c.getValue();
        }
        return ans;
    }

    // key value in the map are put in consecutive positions,
    // even index: rounded vote
    // odd index:  movie count
    private static Map<Integer,Integer> getMapIIFromPairs(int[] pairs) {
        Map<Integer,Integer> ans = new TreeMap<>();
        for (int i=0; i<pairs.length; i+=2) {
            ans.put(pairs[i], pairs[i+1]);
        }
        return ans;
    }

    @Override
    public int run(String[] args) throws Exception {
        // Using Tool permit avoiding using GenericOptionsParser
        //  https://hadoop.apache.org/docs/r1.0.4/api/org/apache/hadoop/util/GenericOptionsParser.html
        //  https://hadoop.apache.org/docs/current/api/org/apache/hadoop/util/Tool.html
        JobArgs parsedArgs = parse(args);
        if (parsedArgs == null) {
            help();
            return -1;
        }
        Configuration conf = getConf();
        //conf.set(N_HASH_FUNCTION_PROPERTY, Integer.toString(calculateK(parsedArgs.p)));
        conf.set(HASH_SEEDS_PROPERTY, parsedArgs.getSeedsString());
        // conf.set(BLOOM_SIZE_PROPERTY, Integer.toString(parsedArgs.p));
        printArgs(parsedArgs);
        // Access file system
        // Doc:
        //  https://hadoop.apache.org/docs/current/api/org/apache/hadoop/fs/FileSystem.html
        FileSystem hdfs = FileSystem.get(getConf());
        boolean merge_exists = hdfs.exists(new Path(parsedArgs.merge, _SUCCESS));
        boolean final_exists = hdfs.exists(new Path(parsedArgs.out, _SUCCESS));
        // check if join directory already exists
        System.out.println("Check for existence of path '" + parsedArgs.merge + "': " + merge_exists);
        if (merge_exists) {
            System.out.println("Path exists!");
            System.out.println("Skip first job");
        } else {
            System.out.println("Path does not exists");
            // launch and wait - if error return error and stop
            if (!launchFirstJob(parsedArgs)) {
                System.err.println("First Job failed!");
                return -1;
            }
            System.out.println("First Job completed!");
            
        }
        // count could be performed while perfoming join or in a separate context
        boolean count_exists = hdfs.exists(new Path(parsedArgs.count, _SUCCESS));
        System.out.println("Check for existence of path '" + parsedArgs.count + "': " + count_exists);
        if (count_exists) {
            System.out.println("Skip count (second) jobs");
        } else {
            if (!launchSecondJob(parsedArgs)) {
                System.err.println("Second Job failed!");
                return -1;
            }
            System.out.println("Second Job completed!");
        }
        if (final_exists) {
            System.out.println("Output directory '" + parsedArgs.out + "' already exists, skip third job.");
        } else {
            System.out.println("Read movie count for vote:");
            Map<Integer,Integer> count = readMovieCount(parsedArgs);
            printMovieCount(count);
            Map<Integer,Integer> estimated_size = estimateBFsizes(parsedArgs.p, count);
            printMapII("Estimated BF sizes:", estimated_size);
            int[] bf_size_pairs = getPairsFromMapII(estimated_size);
            conf.set(DISTINCT_BLOOM_SIZE_PROPERTY, intArrayToString(bf_size_pairs));
            if (!launchThirdJob(parsedArgs)) {
                System.err.println("Third Job failed!");
                return -1;
            }
            System.out.println("Third Job completed!");
        }

        return 0;
    }

    // Perform join between the two datasets:
    // Put titles with average vote
    private boolean launchFirstJob(JobArgs args) throws IOException, ClassNotFoundException, InterruptedException {
        // Jobs perfoming Join
        // https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapreduce/Job.html
        Job job = Job.getInstance(getConf(), "cloud-project-join");
        job.setJarByClass(App.class);
        job.setReducerClass(JoinReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // add input format
        MultipleInputs.addInputPath(job, new Path(args.in_titles), TextInputFormat.class, JoinTitleMapper.class);
        MultipleInputs.addInputPath(job, new Path(args.in_ratings), TextInputFormat.class, JoinRatingMapper.class);
        
        // add output paths
        FileOutputFormat.setOutputPath(job, new Path(args.merge));

        job.setOutputFormatClass(TextOutputFormat.class);
        boolean success = job.waitForCompletion(true);
        if (!success) {
            return false;
        }
        // job is now completed, we can safely access counters

        // access hadoop FS
        FileSystem hdfs = FileSystem.get(getConf());
        // check existence of counter folder
        boolean count_exists = hdfs.exists(new Path(args.count, _SUCCESS));
        if (count_exists) {
            System.err.println("Count directory exists! Count skipped!");
        } else {
            // create directory where to store counters
            Iterator<Counter> it = job.getCounters().getGroup(movie_counter).iterator();
            // to avoid explicit sorting...
            Map<Integer, Long> counter_map = new TreeMap<>();
            while (it.hasNext()) {
                Counter counter = it.next();
                long value = counter.getValue();
                if (value != 0) {
                    counter_map.put(Integer.parseInt(counter.getName()), value);
                }
            }
            // create output file to write counters in
            // write text file                
            try (FSDataOutputStream outs = hdfs.create(new Path(args.count, part_r_00000));) {
                // greater capacity for simplity
                StringBuilder str_builder = new StringBuilder(512);
                for (Map.Entry<Integer,Long> pair : counter_map.entrySet()) {
                    str_builder.append(pair.getKey()).append("\t").append(pair.getValue()).append("\n");
                }
                byte[] bytes = str_builder.toString().getBytes(StandardCharsets.UTF_8);
                outs.write(bytes);
            }
    
            // simulate success
            hdfs.create(new Path(args.count, _SUCCESS)).close();
        }
        return success;
    }

    // Put {rounded vote, 1}
    // Count how many movies have the same rounded vote?
    public static class CounterMapper extends Mapper<LongWritable, Text, ByteWritable, VIntWritable> {
        private ByteWritable outk = new ByteWritable();
        private VIntWritable outv = new VIntWritable();
        private int[] counters;

        @Override
        protected void setup(Mapper<LongWritable, Text, ByteWritable, VIntWritable>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            counters = new int[11];
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, ByteWritable, VIntWritable>.Context context)
                throws IOException, InterruptedException {
            try {
                int rVote = Math.round(Float.parseFloat(value.toString().split("\t")[2]));
                ++counters[rVote];
            } catch (Exception e) {
                return;
            }
        }
        
        @Override
        protected void cleanup(Mapper<LongWritable, Text, ByteWritable, VIntWritable>.Context context)
        throws IOException, InterruptedException {
            for (int i=1; i<=10; ++i) {
                int count = counters[i];
                if (count > 0) {
                    outk.set((byte)i);
                    outv.set(count);
                    context.write(outk, outv);
                }
            }
            super.cleanup(context);
        }
    }

    public static class CounterReducer extends Reducer<ByteWritable, VIntWritable, ByteWritable, VIntWritable> {
        private VIntWritable outv = new VIntWritable();
        protected void reduce(ByteWritable key, Iterable<VIntWritable> iterv, Reducer<ByteWritable,VIntWritable,ByteWritable,VIntWritable>.Context context) throws IOException ,InterruptedException {
            int sum = 0;
            for (VIntWritable v : iterv) {
                sum += v.get();
            }
            outv.set(sum);
            context.write(key, outv);
        }
    }

    // Count number of movies with same rounded average vote
    private boolean launchSecondJob(JobArgs args) throws IOException, ClassNotFoundException, InterruptedException {
        // Jobs perfoming Join
        // https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapreduce/Job.html
        Job job = Job.getInstance(getConf(), "cloud-project-count");
        job.setJarByClass(App.class);
        job.setMapperClass(CounterMapper.class);
        job.setCombinerClass(CounterReducer.class);
        job.setReducerClass(CounterReducer.class);

        job.setMapOutputKeyClass(ByteWritable.class);
        job.setMapOutputValueClass(VIntWritable.class);
        job.setOutputKeyClass(ByteWritable.class);
        job.setOutputValueClass(VIntWritable.class);
    
        // add input/output paths
        NLineInputFormat.addInputPath(job, new Path(args.merge));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", args.nlines);

        FileOutputFormat.setOutputPath(job, new Path(args.count));
    
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return job.waitForCompletion(true);
    }

    // used to rely on MapReduce implementation
    //  https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/WritableComparable.html
    public static class IntPair implements WritableComparable<IntPair> {
        private int first;
        private int second;

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
        public int compareTo(IntPair arg0) {
            int cmp = compare(first, arg0.first);
            if (cmp != 0) {
                return cmp;
            }
            return compare(second, arg0.second);
        }

        private static int compare(int a, int b) {
            return a-b;
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
            return first + "\t" + second;
        }
    }

    // put value as key to keep only uniques
    public static class UniqueMapper extends Mapper<LongWritable, Text, ByteWritable, VIntWritable> {
        private int[] hash_seeds;
        //private int bloom_size;
        // filter for vote v has size movie_bloom_size[v]
        private Map<Integer,Integer> movie_bloom_size;
        @Override
        protected void setup(Mapper<LongWritable, Text, ByteWritable, VIntWritable>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            hash_seeds = conf.getInts(HASH_SEEDS_PROPERTY);
            //bloom_size = conf.getInt(BLOOM_SIZE_PROPERTY, DEFAULT_BLOOM_SIZE);
            movie_bloom_size = getMapIIFromPairs(conf.getInts(DISTINCT_BLOOM_SIZE_PROPERTY));
        }

        private Hash hasher = MurmurHash.getInstance();
        private ByteWritable outk = new ByteWritable();
        private VIntWritable outv = new VIntWritable();
        @Override
        public void map(LongWritable key, Text value, Mapper<LongWritable,Text,ByteWritable,VIntWritable>.Context context) throws IOException ,InterruptedException {
            String[] line = value.toString().split("\t");
            int bloom_size;
            try {
                int rounded_vote = Math.round(Float.parseFloat(line[2]));
                // key is vote rounded
                outk.set((byte)rounded_vote);
                bloom_size = movie_bloom_size.get(rounded_vote);
            } catch(Exception e) {
                return;
            }
            byte[] bytes = line[0].toLowerCase().getBytes(StandardCharsets.UTF_8);
            for (int k : hash_seeds) {
                outv.set((int)(((long)(hasher.hash(bytes, k)%bloom_size)+bloom_size)%bloom_size));
                context.write(outk, outv);
            }
        }
    }

    // calculate indexes of set bits in the Bloom Filters
    private boolean launchThirdJob(JobArgs args) throws IOException, ClassNotFoundException, InterruptedException {
        // Jobs perfoming Join
        // https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/mapreduce/Job.html
        Job job = Job.getInstance(getConf(), "cloud-project-indexes");
        job.setJarByClass(App.class);
        job.setMapperClass(UniqueMapper.class);
        // default should work
        job.setReducerClass(BFReducer.class);
    
        job.setMapOutputKeyClass(ByteWritable.class);
        job.setMapOutputValueClass(VIntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
    
        // add input/output paths
        NLineInputFormat.addInputPath(job, new Path(args.merge));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", args.nlines);

        FileOutputFormat.setOutputPath(job, new Path(args.out));
    
        job.setInputFormatClass(TextInputFormat.class);
        // From the doc:
        //  Using MultipleOutputs in this way will still create zero-sized default output,
        //  eg part-00000. To prevent this use
        //  LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class); instead of
        //  job.setOutputFormatClass(TextOutputFormat.class);
        //  in your Hadoop job configuration.
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        boolean success = job.waitForCompletion(true);
        if (!success) {
            return false;
        }
        // buffer used to generate files
        StringBuilder str_builder = new StringBuilder(512);
        // access hadoop FS
        FileSystem hdfs = FileSystem.get(getConf());
        // generate file containing keys used by the hash functions
        try (FSDataOutputStream outs = hdfs.create(new Path(args.out, bf_h_seeds));) {
            for (Integer h_key : args.seeds) {
                str_builder.append(Integer.toString(h_key)).append("\n");
            }
            // write to file
            byte[] bytes = str_builder.toString().getBytes(StandardCharsets.UTF_8);
            outs.write(bytes);
            // clear to reuse
            str_builder.delete(0, str_builder.length());
        }
        // generate file containing the requested false positive rate
        try (FSDataOutputStream outs = hdfs.create(new Path(args.out, expected_fp_rate));) {
            str_builder.append(Double.toString(args.p)).append("\n");
            // write to file
            byte[] bytes = str_builder.toString().getBytes(StandardCharsets.UTF_8);
            outs.write(bytes);
            // clear to reuse
            str_builder.delete(0, str_builder.length());
        }
        // generate file containing (rounded vote, bf size) pairs
        try (FSDataOutputStream outs = hdfs.create(new Path(args.out, bloom_filter_sizes));) {
            Map<Integer,Integer> movie_bloom_size = getMapIIFromPairs(getConf().getInts(DISTINCT_BLOOM_SIZE_PROPERTY));
            for (Map.Entry<Integer,Integer> c : movie_bloom_size.entrySet()) {
                str_builder.append(Integer.toString(c.getKey())).append("\t").append(Integer.toString(c.getValue())).append("\n");
            }
            // write to file
            byte[] bytes = str_builder.toString().getBytes(StandardCharsets.UTF_8);
            outs.write(bytes);
            // clear to reuse
            str_builder.delete(0, str_builder.length());
        }
        return success;
    }

    /** Last part of the project - build bloom filters from indexes **/

    // Basic class to generate BloomFilters
    public static class BloomFilter {
        private int rating;
        private int N;
        private BitSet bs;
        private int[] seeds;

        public BloomFilter(int rating, int N, int[] seeds) {
            this.rating = rating;
            this.N = N;
            this.bs = new BitSet(N);
            this.seeds = seeds;
        }

        public BloomFilter set(int i) {
            this.bs.set(i);
            return this;
        }

        private String getBase64() {
            Base64.Encoder enc = Base64.getEncoder();
            byte[] bytes = this.bs.toByteArray();
            return enc.encodeToString(bytes);
        }

        public String toJson() {
            StringBuilder str_builder = new StringBuilder(512 << 8).append("{");
            // add vote
            str_builder.append("\"rating\":").append(Integer.toString(rating)).append(",");
            // specify hash function
            str_builder.append("\"hash_function\":\"mmh2\"").append(",");
            // filter lenght
            str_builder.append("\"filter_lenght\":").append(Integer.toString(N)).append(",");
            // add hash seeds
            str_builder.append("\"hash_seeds\":").append(Arrays.toString(seeds)).append(",");
            // add bit vector
            str_builder.append("\"bitvector\":\"").append(getBase64()).append("\"");
            return str_builder.append("}").toString();
        }
    }

    public static class BFReducer extends Reducer<ByteWritable, VIntWritable, NullWritable, Text> {
        private int[] hash_keys;
        private Map<Integer,Integer> movie_bloom_size;
        // Separate files for separate filters
        private MultipleOutputs<NullWritable,Text> mos;

        @Override
        protected void setup(Reducer<ByteWritable,VIntWritable,NullWritable,Text>.Context context)
                throws IOException ,InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            hash_keys = conf.getInts(HASH_SEEDS_PROPERTY);
            int[] pairs = conf.getInts(DISTINCT_BLOOM_SIZE_PROPERTY);
            movie_bloom_size = getMapIIFromPairs(pairs);
            if (movie_bloom_size == null) {
                throw new RuntimeException("Missing property '" + DISTINCT_BLOOM_SIZE_PROPERTY + "' found: " + Arrays.toString(pairs));
            }
            mos = new MultipleOutputs<NullWritable,Text>(context);
        }

        private Text outv = new Text();
        @Override
        protected void reduce(ByteWritable key, Iterable<VIntWritable> interVal, Reducer<ByteWritable,VIntWritable,NullWritable,Text>.Context context)
                throws IOException ,InterruptedException {
            int rounded_vote = key.get();
            int bf_size = movie_bloom_size.get(rounded_vote);
            BloomFilter bf = new BloomFilter(rounded_vote, bf_size, hash_keys);
            for (VIntWritable value : interVal) {
                int i = value.get();
                bf.set(i);
            }
            outv.set(bf.toJson());
            mos.write(NullWritable.get(), outv, "bloom_filter_"+rounded_vote);
        }

        // MultipleOutputs must be explicitly closed
        @Override
        protected void cleanup(Reducer<ByteWritable,VIntWritable,NullWritable,Text>.Context context)
                throws IOException, InterruptedException {
            mos.close();
            super.cleanup(context);
        }
    }
}
