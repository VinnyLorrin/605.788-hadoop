package bdpuh.hadoopfilesystemapi;


import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsDirDelete {
    public static void main(String args[]) {

        Configuration configuration = new Configuration();
  
        Path dirToDelete = new Path("/data");
        boolean recursively = true;
 
        // Get a copy of FileSystem Object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsDirDelete.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Delete a Directory
        boolean status = false;
        try {
            status = fileSystem.delete(dirToDelete, recursively);
        } catch (IOException ex) {
            Logger.getLogger(HdfsDirDelete.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println ("Deleted dir successfully: " +  status);
        
        
    }
    
}
