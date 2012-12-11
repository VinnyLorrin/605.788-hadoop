/*
 * Andrew Stewart
 * Assignment 2.
 */
package parallellocaltohdfscopy;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;
import org.apache.hadoop.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.vafer.jdeb.shaded.compress.compress.utils.IOUtils;
//import org.codehaus.jackson.JsonFactory;
/**
 *
 * @author hadoop
 */
public class ParallelLocalToHdfsCopy {
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        // TODO code application logic here
        String localDir = args[0];
        String remoteDir = args[1];
        int threads = Integer.parseInt(args[2]);

        Configuration configuration = new Configuration();
        configuration.addResource("core-site.xml");
        configuration.addResource(new Path("/usr/local/hadoop/hadoop-1.0.3/conf/hdfs-site.xml"));
        configuration.addResource(new Path("/usr/local/hadoop/hadoop-1.0.3/conf/mapred-site.xml"));        
        
        // FOR TESTING:
        //localDir = "/home/hadoop/Desktop/tmp/";
        //remoteDir = "/tmp/test/";
        //threads = 3;
        
        // test localDir
        File ldir = new File(localDir);
        if(!ldir.isDirectory()) {
            System.out.println("Error: " + localDir + " does not exist!");
        }
 
        // Get a copy of hdfs File System Object
        FileSystem hdfs = FileSystem.get(configuration);

        // test remoteDir
        Path rdir = new Path(remoteDir);
        boolean status = false;
        status = hdfs.mkdirs(rdir);
        if(status == false) {
            System.out.println("Error: couldn't create directorty " + remoteDir);
        }

        // get filelist from local directory
        File[] lfiles = ldir.listFiles();
        
        // create thread pool
        ExecutorService pool = Executors.newFixedThreadPool(threads);
                
        InputStream in = null;
        OutputStream out = null;
        
        // iterate through file list and submit to thread pool
        for(int i = 0; i < lfiles.length; i++) {
            in = new FileInputStream(lfiles[i]);
            System.out.print("Copying " + lfiles[i].getName());
            Path newFile = new Path(rdir.toString() + "/" + lfiles[i].getName());
            System.out.println(" to " + newFile.toString() + "...");
            out = hdfs.create(newFile);
            pool.execute(new hdfsCopyThread(in, out));
        }            

        // shut down thread pool
        pool.shutdown();
               
        // close file system
        hdfs.close();        
    }
}

class hdfsCopyThread implements Runnable {
    private InputStream in = null;
    private OutputStream out = null;
        
    public hdfsCopyThread(InputStream in, OutputStream out) {
        this.in = in;
        this.out = out;
    }

    public void run() {
        try {
            IOUtils.copy(in, out);
        } catch (IOException ex) {
            Logger.getLogger(hdfsCopyThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
