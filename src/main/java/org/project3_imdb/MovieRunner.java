package org.project3_imdb;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class MovieRunner {
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(MovieRunner.class);
        conf.setJobName("movie genre imdb");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);
        conf.setMapperClass(MovieMapper.class);
        conf.setCombinerClass(MovieReducer.class);
        conf.setReducerClass(MovieReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}

