package bdpuh.hadoopfilesystemapi;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsDirRead {

    public static void main(String args[]) {

        Configuration configuration = new Configuration();
        configuration.addResource("core-site.xml");
        configuration.addResource(new Path("/usr/local/hadoop/hadoop-1.0.3/conf/hdfs-site.xml"));
        configuration.addResource(new Path("/usr/local/hadoop/hadoop-1.0.3/conf/mapred-site.xml"));
        
        Path dirToRead = new Path("/");

        // Get a copy of File System Object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsDirRead.class.getName()).log(Level.SEVERE, null, ex);
        }

        FileStatus[] fileStatuses = null;
        
        try {
            fileStatuses = fileSystem.listStatus(dirToRead);
        } catch (IOException ex) {
            Logger.getLogger(HdfsDirRead.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Read records records from the file
        for (int i = 0; i < fileStatuses.length; i++) {
            System.out.println(fileStatuses[i].getPath().getName() + " " + (fileStatuses[i].isDir()? "dir":"file"));
        }
        
        // Close the FileSystem
        try {
            fileSystem.close();
        } catch (IOException ex) {
            Logger.getLogger(HdfsDirRead.class.getName()).log(Level.SEVERE, null, ex);
        }
                
        System.out.println("Read dir successfully");


    }
}
