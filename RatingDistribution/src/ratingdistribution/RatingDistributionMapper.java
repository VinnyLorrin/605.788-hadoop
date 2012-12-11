/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ratingdistribution;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;

public class RatingDistributionMapper extends Mapper<LongWritable, Text, Text, IntWritable>{ 
    
    Logger logger = Logger.getLogger(RatingDistributionMapper.class);
    IntWritable one = new IntWritable(1);
    Text word = new Text();

    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        String fileName = ((FileSplit) context.getInputSplit()).getPath() + "";
        System.out.println ("in stdout"+ context.getTaskAttemptID().toString() + " " +  fileName);
        System.err.println ("in stderr"+ context.getTaskAttemptID().toString());
    }
    
    
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
//        line = line.replaceAll("[,.'\"!\\/]", "");
        StringTokenizer tokenizer = new StringTokenizer(line,"\t");
//        while (tokenizer.hasMoreTokens()) {
//            word.set(tokenizer.nextToken());
//            context.write(word, one);
//        }
        tokenizer.nextToken();
        tokenizer.nextToken();
        word.set(tokenizer.nextToken());
        context.write(word,one);
    }   
}