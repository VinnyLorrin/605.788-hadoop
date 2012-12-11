/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package movieratings;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

/**
 *
 * @author hadoop
 */
public class MovieRatingsMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    Logger logger = Logger.getLogger(MovieRatingsMapper.class);
    Text item = new Text();
    Text fields = new Text();
    
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
        StringTokenizer tokenizer = new StringTokenizer(line,"\t");
        if(tokenizer.countTokens() == 4) {  // u.data record
            tokenizer.nextToken();
            String itemid = tokenizer.nextToken();
            String rating = tokenizer.nextToken();
            item.set(itemid);
            fields.set(rating);
            context.write(item,fields);
        }
        else { // u.item record
            tokenizer = new StringTokenizer(line,"|");
            String itemid = tokenizer.nextToken();
            String title = tokenizer.nextToken();
            String release = tokenizer.nextToken();
            //tokenizer.nextToken();
            String imdb = tokenizer.nextToken();
            fields.set(title + "\t" + release + "\t" + imdb);
            item.set(itemid);
            context.write(item,fields);
        }

        //TotalRecords counter
        Counter counter = context.getCounter("MyCounter","TOTALRECORDS");
        counter.increment(1);
    }   
}
