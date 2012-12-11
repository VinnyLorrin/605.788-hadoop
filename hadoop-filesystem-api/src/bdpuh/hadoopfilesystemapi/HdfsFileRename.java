package bdpuh.hadoopfilesystemapi;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileRename {
    public static void main(String args[]) {

        Configuration configuration = new Configuration();
 
        Path fileToRename = new Path("/records.txt");
        Path newName = new Path("/details.txt");
      
        // Get a copy of FileSystem Object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileRename.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Rename the File
        boolean done = false;
        try {
           done = fileSystem.rename(fileToRename,newName);                    
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileRename.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // Close the FileSystem
        try {
            fileSystem.close();
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileRename.class.getName()).log(Level.SEVERE, null, ex);
        }
        

        System.out.println ("Renamed file successfully: " + done);
        
        
        
    }
    
}
