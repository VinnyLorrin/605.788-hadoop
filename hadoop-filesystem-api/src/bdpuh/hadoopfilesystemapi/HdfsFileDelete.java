package bdpuh.hadoopfilesystemapi;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileDelete {
    public static void main(String args[]) {

        Configuration configuration = new Configuration();
 
        Path fileToDelete = new Path("/metrics.txt");
      
        // Get a copy of File System Object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileDelete.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Delete the File
        boolean done = false;
        FSDataOutputStream fSDataOutputStream = null;
        try {
           done = fileSystem.delete(fileToDelete,false);                    
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileDelete.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // Close the FileSystem
        try {
            fileSystem.close();
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileDelete.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println ("Deleted file successfully: " + done);
        
        
        
    }
    
}
