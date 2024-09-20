package edu.cs.utexas.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends  Reducer<Text, Text, Text, Text> {

   public void reduce(Text text, Iterable<Text> values, Context context)
           throws IOException, InterruptedException {
	   
        int numFlight = 0;
        float totalDelay = 0;
       
        for (Text value : values) {
            String[] splat = value.toString().split(" ");
            if (splat.length == 2) {
                try {
                    numFlight += Integer.parseInt(splat[0]);
                    totalDelay += Float.parseFloat(splat[1]);
                } catch (NumberFormatException e) {
                    continue;
                } // do nun
            }
        }
       String temp = numFlight + " " + totalDelay;
        context.write(text, new Text(temp));
    }
}