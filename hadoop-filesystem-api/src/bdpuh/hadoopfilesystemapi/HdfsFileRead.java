package bdpuh.hadoopfilesystemapi;


import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsFileRead {
    
    public static void main(String args[]) {
        
        Configuration configuration = new Configuration();
        
        Path fileToRead = new Path("/records.txt");
                
        // Get a copy of FileSystem Object
        FileSystem fileSystem = null;       
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileRead.class.getName()).log(Level.SEVERE, null, ex);
        }


        // Open a File for Reading
        FSDataInputStream fSDataInputStream = null;        
        try {
            fSDataInputStream = fileSystem.open(fileToRead);
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileRead.class.getName()).log(Level.SEVERE, null, ex);
        }

        
        try {
            // Read characters from the file until eof
            int aChar;
            aChar =  fSDataInputStream.read();
            while ((aChar = fSDataInputStream.read()) != -1) {
                System.out.print ((char) aChar);
            }        
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileRead.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        // Close the InputStream and FileSystem
        try {
            fSDataInputStream.close();
            fileSystem.close();
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileRead.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println ("Read file successfully");
        
        
    }
    
}
