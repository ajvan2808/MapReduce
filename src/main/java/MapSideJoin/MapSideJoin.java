package MapSideJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MapSideJoin {
    public static class TransMapper extends Mapper<Object, Text, Text, Text> {
        // Use map to store key-value data userId, userName at setUp
        private Map<Integer, String> userMap = new HashMap<>();

        public void setUp(Context context) throws IOException, InterruptedException {
            try (BufferedReader buffReader = new BufferedReader(new FileReader("cust.txt"))) {
                String line;
                while((line = buffReader.readLine()) != null){
                    String columns[] = line.split(",");
                     String ID = columns[0];
                     String Name = columns[1];
                    userMap.put(Integer.parseInt(ID), Name);
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        // mapper function
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String records = value.toString().trim();
            String[] parts = records.split(",");
            String gametype = parts[4];
            String id = parts[2];

            // get name from cache in setUp by id in larger file
            String name = userMap.get(Integer.parseInt(id));

            context.write(new Text(id), new Text(name + " " + gametype));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapSide Join");
        job.setJarByClass(MapSideJoin.class);
        job.setMapperClass(TransMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // Setting reducer to zero
        // job.setNumReduceTasks(0);

        try {
            DistributedCache.addCacheFile(new URI("hdfs://localhost:8020/mycache/cust.txt"), conf);
        }
        catch (Exception e) {
            System.out.println("File not added");
            System.exit(1);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
