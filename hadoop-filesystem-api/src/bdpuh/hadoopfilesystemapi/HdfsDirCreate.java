package bdpuh.hadoopfilesystemapi;


import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsDirCreate {
    public static void main(String args[]) {
        
        Configuration configuration = new Configuration();
        configuration.addResource("core-site.xml");
        configuration.addResource(new Path("/usr/local/hadoop/hadoop-1.0.3/conf/hdfs-site.xml"));
        configuration.addResource(new Path("/usr/local/hadoop/hadoop-1.0.3/conf/mapred-site.xml"));       

        Path dir = new Path("/data/subdir/subsubdir");
//       Path dir = new Path("/tmp/data");
 
        // Get a copy of FileSystem Object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsDirCreate.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Create a Directory
        boolean status = false;
        try {
            status = fileSystem.mkdirs(dir);
        } catch (IOException ex) {
            Logger.getLogger(HdfsDirCreate.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        // Close the FileSystem
        try {
            fileSystem.close();
        } catch (IOException ex) {
            Logger.getLogger(HdfsDirCreate.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println ("Created dir successfully: " + status);               
    }
    
}
