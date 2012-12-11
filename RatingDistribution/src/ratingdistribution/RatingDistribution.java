/*
 * Andrew Stewart
 */
package ratingdistribution;

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
public class RatingDistribution {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // Input parameters
        String indir = args[0];
        String outdir = args[1];
        
        // hadoop configuration
//        Configuration configuration = new Configuration();
//        configuration.addResource("core-site.xml");
//        configuration.addResource(new Path("/usr/local/hadoop/hadoop-1.0.3/conf/hdfs-site.xml"));
//        configuration.addResource(new Path("/usr/local/hadoop/hadoop-1.0.3/conf/mapred-site.xml"));        
        
        // configure job
        Job movieRatingsJob = null;
        
        try {
            movieRatingsJob = new Job(new Configuration(), "MovieRatings");
        } catch (IOException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        // Specify the Input path
        try {
            FileInputFormat.addInputPath(movieRatingsJob, new Path(indir));
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(RatingDistribution.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
            return;
        }

        // Set the Input Data Format
        movieRatingsJob.setInputFormatClass(TextInputFormat.class);
        
        // Set the Mapper and Reducer Class
        movieRatingsJob.setMapperClass(RatingDistributionMapper.class);
        movieRatingsJob.setReducerClass(RatingDistributionReducer.class);

        // Set the Jar file 
        movieRatingsJob.setJarByClass(ratingdistribution.RatingDistribution.class);
        
        // Set the Output path
        FileOutputFormat.setOutputPath(movieRatingsJob, new Path(outdir));
        
        // Set the Output Data Format
        movieRatingsJob.setOutputFormatClass(TextOutputFormat.class);        
        
        // Set the Output Key and Value Class
        movieRatingsJob.setOutputKeyClass(Text.class);
        movieRatingsJob.setOutputValueClass(IntWritable.class); 
             
        try {
            movieRatingsJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
        }

        
    }
}
