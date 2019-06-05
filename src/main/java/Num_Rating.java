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

public class Num_Rating {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        //number_rating movie/rating

        private Text gender = new Text();
        private IntWritable age = new IntWritable();
        private Text category = new Text();
        private Text video_name = new Text();
        String video;
        Float rate;
        private  IntWritable num_rating = new IntWritable();
        private  FloatWritable rating = new FloatWritable();
        private  Text combine = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String str[]=line.split("\t");
            String num_rating_text = "";
            String rating_text = "";
            if(str.length > 7){
                video_name.set(str[0]);
                if(str[6].matches("\\d+.+") && str[7].matches("\\d+")){ //this regular expression
                    int f=Integer.parseInt(str[7]);
                    num_rating.set(f);
                    num_rating_text = str[7];
                    //float f=Float.parseFloat(str[6]); //typecasting string to float
                    rating.set(f);
                    rating_text = str[6];

                }

                combine.set(new Text(num_rating_text+"/"+rating_text));
            }

            context.write(video_name, combine);

        }
    }

    public static class Reduce extends Reducer<Text,Text, IntWritable, Text> {
        private TreeMap<Integer,String> tmap = new TreeMap<Integer, String>();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int num_of_rating = 0;
            int l=0;
            String rate ="";
            for (Text val : values) {
                String[] tokens = val.toString().split("/");
                if(tokens.length == 2){
                    num_of_rating += Integer.parseInt(tokens[0]);
                    rate = tokens[1];
                    tmap.put(num_of_rating,key.toString()+","+rate);
                }
            }
               //takes the average of the sum
            //context.write(key, new FloatWritable(sum));

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

                Integer count = entry.getKey();
                String value = entry.getValue();
                context.write(new IntWritable(count),new Text(value));


            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "youtube_rating");
        job.setJarByClass(Num_Rating.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //  job.setNumReduceTasks(0);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Num_Rating.Map.class);
        job.setReducerClass(Num_Rating.Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        Path out=new Path(args[1]);
        out.getFileSystem(conf).delete(out);
        job.waitForCompletion(true);
    }
}
