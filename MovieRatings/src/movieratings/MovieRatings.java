/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package movieratings;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author hadoop
 */
public class MovieRatings {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        // Input parameters
        String indir = args[0];
        String outdir = args[1];
        
        // hadoop configuration
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource(new Path("/usr/local/hadoop/hadoop-1.0.3/conf/hdfs-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/hadoop-1.0.3/conf/mapred-site.xml"));     
        
        // Job Input Compression
        conf.setStrings("io.compression.codecs",
                "org.apache.hadoop.io.compress.GzipCodec");     
        
        // Job Output Compression
        conf.setBoolean("mapred.output.comress", true);
        conf.setStrings("mapred.output.compression.codec",
                "org.apache.hadoop.io.compress.GzipCodec");
        
        // configure job
        Job movieRatingsJob = null;
        
        try {
            movieRatingsJob = new Job(conf, "MovieRatings");
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        // Specify the Input path
        try {
            FileInputFormat.addInputPath(movieRatingsJob, new Path(indir));
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(MovieRatings.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            return;
        }

        // Set the Input Data Format
        movieRatingsJob.setInputFormatClass(TextInputFormat.class);
        
        // Set the Mapper and Reducer Class
        movieRatingsJob.setMapperClass(MovieRatingsMapper.class);
        movieRatingsJob.setReducerClass(MovieRatingsReducer.class);

        // Set the Jar file 
        movieRatingsJob.setJarByClass(movieratings.MovieRatings.class);
        
        // Set the Output path
        FileOutputFormat.setOutputPath(movieRatingsJob, new Path(outdir));
        
        // Set the Output Data Format
        movieRatingsJob.setOutputFormatClass(TextOutputFormat.class);        
        
        // Set the Output Key and Value Class
        movieRatingsJob.setOutputKeyClass(Text.class);
        //movieRatingsJob.setOutputValueClass(IntWritable.class);
        movieRatingsJob.setOutputValueClass(Text.class);
        
        
        movieRatingsJob.setNumReduceTasks(2);
        
        try {
            movieRatingsJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
