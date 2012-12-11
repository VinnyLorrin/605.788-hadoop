package bdpuh.hadoopfilesystemapi;


import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileCreate {
    public static void main(String args[]) {

        Configuration configuration = new Configuration();

        Path fileToCreateAndWrite = new Path("/records.txt");

        // Get a copy of HDFS FileSystem Object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileCreate.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Create a File for writing
        FSDataOutputStream fSDataOutputStream = null;
        try {
            fSDataOutputStream = fileSystem.create(fileToCreateAndWrite);
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileCreate.class.getName()).log(Level.SEVERE, null, ex);
        }

        
        String record1 = "This is record 001\n";
        String record2 = "This is record 002\n";
        String record3 = "This is record 003\n";
        try {
            // Write String records to the file
             fSDataOutputStream.writeChars(record1);
             fSDataOutputStream.writeChars(record2);
             fSDataOutputStream.writeChars(record3);
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileCreate.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        //Close the OutputStream and FileSystem
        try {
            fSDataOutputStream.close();
            fileSystem.close();
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileCreate.class.getName()).log(Level.SEVERE, null, ex);
        }


        System.out.println ("Created and writen to file successfully");
        
        
    }
    
}
