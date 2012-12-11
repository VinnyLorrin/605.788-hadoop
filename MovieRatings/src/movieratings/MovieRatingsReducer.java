/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package movieratings;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author hadoop
 */
public class MovieRatingsReducer extends Reducer<Text, Text, Text, Text> {
    int i = 0;
    int x = 0;
    String t = "\t";
    Text fields = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    
        i = 0;
        x = 0;
        for(Text val : values) {   //IntWritable should be Text(and above)
            String line = val.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,"\t");
            
            if(tokenizer.countTokens() == 3) {  // u.item record
                String title = tokenizer.nextToken();            
                String release = tokenizer.nextToken();
                String imdb = tokenizer.nextToken();
                fields.set(title + t + release + t + imdb);
            }
            else { // u.data record
                int rating = Integer.parseInt(tokenizer.nextToken());
                x += rating;
                i = i + 1;
            }
            
        }
        
        double avg = (double)x/i;
        fields.set(fields.toString() + t + String.format("%.2f", avg) + t + Integer.toString(i));
        
        context.write (key, fields);
        
        //UniqueMovies counter
        Counter counter = context.getCounter("MyCounter","UNIQUEMOVIES");
        counter.increment(1);
    }       
}
