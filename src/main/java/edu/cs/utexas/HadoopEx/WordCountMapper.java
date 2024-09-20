package edu.cs.utexas.HadoopEx;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<Object, Text, Text, Text> {

	// Create a hadoop text object to store words
	private Text word = new Text();
	private Text delayText = new Text();

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] splat = value.toString().split(",");
		if (splat.length > 11) {
			float delay = 0;
			try {
				delay = Float.parseFloat(splat[11]);
			} catch (NumberFormatException e) {
				delay = 0;
			}
			word.set(splat[4]);
			delayText.set("1 " + delay);
			context.write(word, delayText);
		}
		
	}
}