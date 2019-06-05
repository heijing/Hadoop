import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.TreeMap;

public class Num_Rating_Count {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text gender = new Text();
        private IntWritable age = new IntWritable();
        private Text category = new Text();
        private Text video_name = new Text();
        String video;
        Float rate;
        private  FloatWritable rating = new FloatWritable();
        private  IntWritable num_of_rating = new IntWritable();
        //private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String str[]=line.split("\t");

            if(str.length > 7){
                video_name.set(str[0]);
                if(str[7].matches("\\d+")){ //this regular expression
                    int f=Integer.parseInt(str[7]); //typecasting string to float
                    num_of_rating.set(f);
                }
            }

            context.write(video_name, num_of_rating);
//            }

        }
    }

    public static class Reduce extends Reducer<Text,IntWritable, Text, IntWritable> {
        private TreeMap<Integer,String> tmap = new TreeMap<Integer, String>();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int l=0;
            for (IntWritable val : values) {
                //l+=1;  //counts number of values are there for that key
                sum += val.get();
            }
            //sum=sum/l;   //takes the average of the sum
            //context.write(key, new FloatWritable(sum));
            tmap.put(sum,key.toString());
            if(tmap.size() > 10){
                tmap.remove(tmap.firstKey());
            }
        }
        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException
        {

            for (java.util.Map.Entry<Integer, String> entry : tmap.entrySet())
            {

                int count = entry.getKey();
                String name = entry.getValue();
                context.write(new Text(name), new IntWritable(count));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "youtube_rating");
        job.setJarByClass(Num_Rating_Count.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //  job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Num_Rating_Count.Map.class);
        job.setReducerClass(Num_Rating_Count.Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out=new Path(args[1]);
        out.getFileSystem(conf).delete(out);
        job.waitForCompletion(true);
    }

}
