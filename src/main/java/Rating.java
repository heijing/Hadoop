import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

public class Rating {
    public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private Text gender = new Text();
        private IntWritable age = new IntWritable();
        private Text category = new Text();
        private Text video_name = new Text();
        String video;
        Float rate;
        private  FloatWritable rating = new FloatWritable();
        //private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String str[]=line.split("\t");

            if(str.length > 7){
                video_name.set(str[0]);
                if(str[6].matches("\\d+.+")){ //this regular expression
                    float f=Float.parseFloat(str[6]); //typecasting string to float
                    rating.set(f);
                }
            }

            context.write(video_name, rating);
//            String str[] = line.split("\t");
//            video = (str[0]);
//            if(str[6].matches("\\d+.+")) {
//                float f = Float.parseFloat(str[6]);
//                rate = f;
//            }
//            tmap.put(video,rate);
//            if(tmap.size() > 10){
//                tmap.remove(tmap.firstKey());
//            }
//            for (java.util.Map.Entry<String, Float> entry : tmap.entrySet())
//            {
//
//                String count = entry.getKey();
//                Float name = entry.getValue();
//
//                context.write(new Text(count), new FloatWritable(name));
//            }

        }
    }

    public static class Reduce extends Reducer<Text,FloatWritable, Text, FloatWritable> {
        private TreeMap<Float,String> tmap = new TreeMap<Float, String>();
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int l=0;
            for (FloatWritable val : values) {
                l+=1;  //counts number of values are there for that key
                sum += val.get();
            }
            sum=sum/l;   //takes the average of the sum
            //context.write(key, new FloatWritable(sum));
            tmap.put(sum,key.toString());
            if(tmap.size() > 10){
            tmap.remove(tmap.firstKey());
            }
//            for (java.util.Map.Entry<String, Float> entry : tmap.entrySet())
//            {
//
//               String count = entry.getKey();
//                Float name = entry.getValue();
//
//                context.write(new Text(count), new FloatWritable(name));
//            }

            //context.write(key, new FloatWritable(sum));
        }
        @Override
        public void cleanup(Context context) throws IOException,
                InterruptedException
        {

            for (java.util.Map.Entry<Float, String> entry : tmap.entrySet())
            {

               Float count = entry.getKey();
                String name = entry.getValue();
                context.write(new Text(name), new FloatWritable(count));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "youtube");
        job.setJarByClass(Rating.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FloatWritable.class);
        //  job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out=new Path(args[1]);
        out.getFileSystem(conf).delete(out);
        job.waitForCompletion(true);
    }

        //Read more: https://javarevisited.blogspot.com/2012/12/how-to-sort-hashmap-java-by-key-and-value.html#ixzz5pSnEyKc6
}
