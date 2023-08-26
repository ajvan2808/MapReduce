package midTerm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class MapJoin {
    public static class TransMapper extends Mapper<Object, Text, Text, Text> {
        // Use map to store key-value data userId, userName at setUp
        protected Map<String, String> userMap = new HashMap<>();

        public void setUp(Context context) throws IOException, InterruptedException {
            try (Scanner buffReader = new Scanner(new FileReader("cust.txt"))) {
                String line;
                while (buffReader.hasNextLine()) {
                    line = buffReader.nextLine();
                    String[] columns = line.split(",");
                    String ID = columns[0];
                    String Name = columns[1];
                    userMap.put(ID, Name);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // mapper function
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String records = value.toString().trim();
            String[] parts = records.split(",");
            String dateTime = parts[1];
            String month = dateTime.substring(0,3);
            String id = parts[2];

            // get name from cache in setUp by id in larger file
            String name = userMap.get(id);
            if (name != null) {
                context.write(new Text(month), new Text(name));
            } else {
                userMap.put(id, "Unknown");
            }

        }
    }

//    public static class transReducer extends Reducer<Text, Text, Text, Text> {
//        private Map<String, Integer> userPlayCount = new HashMap<String, Integer>();
//        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            String entryStr ="";
//            for (Text t : values) {
//                if (!userPlayCount.containsKey(t.toString())) {
//                    userPlayCount.put(t.toString(), 0);
//                } else {
//                    userPlayCount.put(t.toString(), userPlayCount.get(t.toString() + 1));
//                }
//                entryStr += " " + t.toString() + userPlayCount.get(t.toString());
//            }
//
//            context.write(key, new Text(entryStr));
//        }
//    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapSide Join");
        job.setJarByClass(MapJoin.class);
        job.setMapperClass(TransMapper.class);
//        job.setReducerClass(transReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Setting reducer to zero
//        job.setNumReduceTasks(2);

        try {
            DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/mycache/cust.txt"), conf);
        } catch (Exception e) {
            System.out.println("File not added");
            System.exit(1);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
