package JobChainReduceSideJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class JobChainReduceSideJoin {
    public static class CustomerMapper extends Mapper<Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            String id = parts[0];
            String age = parts[3];
            context.write(new Text(id), new Text("age " + age));
        }
    }
    // map -> [40001    age 50], [40002    age 60], ...
    // sort & shuffle -> {40001    [(age 50), (age 55), ...]}

    public static class TrxnMapper extends Mapper<Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            String gametype = parts[4];
            String id = parts[2];
            context.write(new Text(id), new Text("type " + gametype));
        }
    }
    // map -> [40001    type Gym], [40002    type Yoga], ...
    // sort & shuffle -> {40001    [(type Gym), (type Yoga), ...]}

    public static class GameTypeOfAgeReducer extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int age_level = 0;
            StringBuilder games = new StringBuilder();

            for (Text category: values) {
                String parts[] = category.toString().split(" ");
                if (parts[0].equals("type")) {
                    games.append(parts[1]).append(",");
                } else if (parts[0].equals("age")) {
                    age_level += Integer.parseInt(parts[1]);
                }
            }

            context.write(new Text(Integer.toString(age_level)), new Text(games.toString()));
        }
    }
    // sort & shuffle -> {40001    [(age 50), (type Gym), (type Yoga), ...]}
    // reduce -> 50    Gym, Yoga, ...

    public static class GameTypeMapper extends Mapper<Object, Text, Text, Text>
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split("\t");
            String age = parts[0];
            String[] gameList = parts[1].split(",");
            for (String g: gameList) {
                String name = g;
                context.write(new Text(name), new Text(age));
            }
        }
    }
    // map -> [Gym    50], ...
    // sort & shuffle -> {Gym    [50, 55, 60, ...]}

    public static class MinMaxAvgReducer extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int counter = 0;
            int total = 0;
            List<Integer> age_values = new ArrayList<Integer>();

            for (Text s: values) {
                age_values.add(Integer.parseInt(s.toString()));
                total += Integer.parseInt(s.toString());
                counter++;
            }
            Integer max_age = Collections.max(age_values);
            Integer min_age = Collections.min(age_values);
            Double avg = (double) (total / counter);

            String rs = String.format("Max: %d - Min: %d - Avg: %f", max_age, min_age, avg);

            context.write(new Text(key), new Text(rs));
        }
    }

    public static  void main(String[] args) throws Exception{
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1, "Reduce-side Join with Job Chain");
        job1.setJarByClass(JobChainReduceSideJoin.class);
        job1.setReducerClass(GameTypeOfAgeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, TrxnMapper.class);

        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job1, outputPath);
        outputPath.getFileSystem(conf1).delete(outputPath);
        job1.waitForCompletion(true);

        // Second job
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "Reduce-side Join with Job Chain");
        job2.setJarByClass(JobChainReduceSideJoin.class);
        job2.setMapperClass(GameTypeMapper.class);
        job2.setReducerClass(MinMaxAvgReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        System.exit(job2.waitForCompletion(true) ? 0: 1);
    }
}
