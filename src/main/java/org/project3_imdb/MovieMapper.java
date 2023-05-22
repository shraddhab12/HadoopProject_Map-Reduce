package org.project3_imdb;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MovieMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);

    public void map(LongWritable longWritable, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
        String[] fields = value.toString().split(";");
        String[] genres = fields[4].split(",");
        String year = fields[3];
        Text word = new Text();


        if (genres.length > 1) {
            if (year.compareTo("2000") >= 0 && year.compareTo("2006") <= 0) {
                for (String genre : genres) {
                    if (genre.equals("Comedy") || genre.equals("Romance")) {
                        output.collect(new Text("[2000-2006]," + "Comedy;Romance"), one);
                        break;
                    }
                }
                for (String genre : genres) {
                    if (genre.equals("Action") || genre.equals("Drama")) {
                        output.collect(new Text("[2000-2006]," + "Action;Drama"), one);
                        break;
                    }
                }
                for (String genre : genres) {
                    if (genre.equals("Adventure") || genre.equals("Sci-Fi")) {
                        output.collect(new Text("[2000-2006]," + "Adventure;Sci-Fi"), one);
                        break;
                    }
                }
            } else if (year.compareTo("2007") >= 0 && year.compareTo("2013") <= 0) {
                for (String genre : genres) {
                    if (genre.equals("Comedy") || genre.equals("Romance")) {
                        output.collect(new Text("[2007-2013]," + "Comedy;Romance"), one);
                        break;
                    }
                }
                for (String genre : genres) {
                    if (genre.equals("Action") || genre.equals("Drama")) {
                        output.collect(new Text("[2007-2013]," + "Action;Drama"), one);
                        break;
                    }
                }
                for (String genre : genres) {
                    if (genre.equals("Adventure") || genre.equals("Sci-Fi")) {
                        output.collect(new Text("[2007-2013]," + "Adventure;Sci-Fi"), one);
                        break;
                    }
                }
            } else if (year.compareTo("2014") >= 0 && year.compareTo("2020") <= 0) {
                for (String genre : genres) {
                    if (genre.equals("Comedy") || genre.equals("Romance")) {
                        output.collect(new Text("[2014-2020]," + "Comedy;Romance"), one);
                        break;
                    }
                }
                for (String genre : genres) {
                    if (genre.equals("Action") || genre.equals("Drama")) {
                        output.collect(new Text("[2014-2020]," + "Action;Drama"), one);
                        break;
                    }
                }
                for (String genre : genres) {
                    if (genre.equals("Adventure") || genre.equals("Sci-Fi")) {
                        output.collect(new Text("[2014-2020]," + "Adventure;Sci-Fi"), one);
                        break;
                    }
                }
            }
        }
    }
}
