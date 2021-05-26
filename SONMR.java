import java.io.*;
import java.net.URI;
import java.nio.file.Files;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SONMR {

    public static class SONMRMapper1
            extends Mapper<Object, Text, Text, NullWritable> {
        int dataset_size;
        int transactions_per_block;
        int min_supp;
        double corr_factor;

        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            dataset_size = conf.getInt("dataset_size", -1);
            transactions_per_block = conf.getInt("transactions_per_block", -1);
            min_supp = conf.getInt("min_supp", -1);
            corr_factor = conf.getDouble("corr_factor", -1.0);

        } //setup()

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            double thres = corr_factor * ((float) min_supp / dataset_size) * transactions_per_block;
            FrequentItemFinder finder = new FrequentItemFinder();
            HashSet<HashSet<Integer>> freqItemsets = finder.findFreqItem(thres, value);
            for (HashSet<Integer> set : freqItemsets) {
                StringBuilder builder = new StringBuilder();
                for (Integer item : set) {
                    builder.append(item);
                    builder.append(" ");
                }
                context.write(new Text(builder.toString()), NullWritable.get());
            }

            while (!freqItemsets.isEmpty()) {
                HashSet<HashSet<Integer>> nonCandidates = new HashSet<>();
                HashSet<HashSet<Integer>> candidates = new HashSet<>();
                for (HashSet<Integer> key1 : freqItemsets) {
                    for (HashSet<Integer> key2 : freqItemsets) {
                        if (key1 != key2) {
                            int k = key1.size();
                            HashSet<Integer> union = new HashSet<>(key1);
                            union.addAll(key2);
                            if (nonCandidates.contains(union) || candidates.contains(union)) {
                                continue;
                            }
                            if (union.size() != k + 1) {
                                nonCandidates.add(union);
                            } else {
                                boolean foundAll = true;
                                for (Integer item : union) {
                                    HashSet<Integer> subset = new HashSet<>(union);
                                    subset.remove(item);
                                    if (!freqItemsets.contains(subset)) {
                                        nonCandidates.add(union);
                                        foundAll = false;
                                        break;
                                    }
                                }
                                if (foundAll) {
                                    candidates.add(union);
                                }
                            }
                        }
                    }
                }
                freqItemsets = finder.findFreqSet(thres, value, candidates);
                for (HashSet<Integer> set : freqItemsets) {
                    StringBuilder builder = new StringBuilder();
                    for (Integer item : set) {
                        builder.append(item);
                        builder.append(" ");
                    }
                    context.write(new Text(builder.toString()), NullWritable.get());
                }

            }
        } //map()
    } //SONMRMapper1

    public static class SONMRReducer1
            extends Reducer<Text, NullWritable, Text, NullWritable> {
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            HashSet<HashSet<Integer>> candidates = new HashSet<>();
            BufferedReader reader = new BufferedReader(new StringReader(key.toString()));
            for (String sw = reader.readLine(); sw != null; sw = reader.readLine()) {
                HashSet<Integer> temp = new HashSet<>();
                for (String w : sw.split("\\s")) {
                    temp.add(Integer.parseInt(w));
                }
                candidates.add(temp);
            }
            for (HashSet<Integer> set : candidates) {
                StringBuilder builder = new StringBuilder();
                for (Integer item : set) {
                    builder.append(item);
                    builder.append(" ");
                }
                context.write(new Text(builder.toString()), NullWritable.get());
            }
        } //reduce()
    } //SONMRReducer1


    public static class SONMRMapper2 extends Mapper<Object, Text, Text, IntWritable> {
        HashSet<HashSet<Integer>> candidates = new HashSet<>();

        public void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            BufferedReader readSet = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFiles[0].toString())));
            for (String sw = readSet.readLine(); sw != null; sw = readSet.readLine()) {
                HashSet<Integer> temp = new HashSet<>();
                for (String w : sw.split("\\s")) {
                    temp.add(Integer.parseInt(w));
                }
                candidates.add(temp);
            }

        } //setup()

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FrequentItemFinder finder = new FrequentItemFinder();
            HashMap<HashSet<Integer>, Integer> results = finder.findSetSupport(value, candidates);
            for (HashSet<Integer> set : results.keySet()) {
                StringBuilder builder = new StringBuilder();
                for (Integer item : set) {
                    builder.append(item);
                    builder.append(" ");
                }
                context.write(new Text(builder.toString()), new IntWritable(results.get(set)));
            }
        } //map()
    } //SONMRMapper2

    public static class SONMRCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        } //reduce()
    } //SONMRCombiner

    public static class SONMRReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();
        private int min_supp;

        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            min_supp = conf.getInt("min_supp", -1);
        } //setup()

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (sum >= min_supp) {
                result.set(sum);
                context.write(key, result);
            }
        } //reduce()
    } //SONMRReducer2()


    public static void main(String[] args) throws Exception {
        int dataset_size = Integer.parseInt(args[0]);
        int transactions_per_block = Integer.parseInt(args[1]);
        int min_supp = Integer.parseInt(args[2]);
        double corr_factor = Double.parseDouble(args[3]);
        Path input_path = new Path(args[4]);
        Path interm_path = new Path(args[5]);
        Path output_path = new Path(args[6]);
        Configuration conf = new Configuration();
        conf.setInt("dataset_size", dataset_size);
        conf.setInt("min_supp", min_supp);
        conf.setDouble("corr_factor", corr_factor);
        conf.setInt("transactions_per_block", transactions_per_block);
        Job job = Job.getInstance(conf, "SONMR");
        org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.setNumLinesPerSplit(job, transactions_per_block);
        job.setInputFormatClass(MultiLineInputFormat.class);
        job.setJarByClass(SONMR.class);
        job.setMapperClass(SONMRMapper1.class);
        job.setReducerClass(SONMRReducer1.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, input_path);
        FileOutputFormat.setOutputPath(job, interm_path);
        job.waitForCompletion(true);
        Job job2 = Job.getInstance(conf, "SONMR2");
        org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.setNumLinesPerSplit(job2, transactions_per_block);
        job2.setInputFormatClass(MultiLineInputFormat.class);
        job2.setJarByClass(SONMR.class);
        job2.setMapperClass(SONMRMapper2.class);
        job2.setCombinerClass(SONMRCombiner.class);
        job2.setReducerClass(SONMRReducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        Path temp = new Path(interm_path.toString() + "/part-r-00000");
        job2.addCacheFile(temp.toUri());
        job2.setOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job2, input_path);
        FileOutputFormat.setOutputPath(job2, output_path);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    } //main()
}