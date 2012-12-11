package bdpuh.hadoopfilesystemapi;


import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsGlobbing {
    
    public static void main(String args[]) {
        
        Configuration configuration = new Configuration();
        
        // Get a copy of File System Object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGlobbing.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Match File Patterns
        Path glob = new Path ("/???");
        FileStatus[] fileStatuses = null;
        
        try {
            fileStatuses = fileSystem.globStatus(glob);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGlobbing.class.getName()).log(Level.SEVERE, null, ex);
        }

        for (FileStatus file : fileStatuses) {
            System.out.println (file.getPath().getName());
        }
        
        // Close the FileSystem
        try {
            fileSystem.close();
        } catch (IOException ex) {
            Logger.getLogger(HdfsGlobbing.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println ("Match File patterns successfully");
        
        
    }
    
}
