package bdpuh.hadoopfilesystemapi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class ProgressableFileCopy {

    public static void main(String args[]) {

        Configuration configuration = new Configuration();

        // Get a copy of HDFS FileSystem Object
        FileSystem hdfsFileSystem = null;
        try {
            hdfsFileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsDirRead.class.getName()).log(Level.SEVERE, null, ex);
        }


        // Get the Source File InputStream
        File sourceFile = new File("/home/hadoop/Downloads/ml-10M100K/ratings.dat");
        FileInputStream sourceFileInputStream = null;
        try {
            sourceFileInputStream = new FileInputStream(sourceFile);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ProgressableFileCopy.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Get the Destination FSDataOutputStream to write
        Path destinationFile = new Path("/ratings.dat");
        FSDataOutputStream destinationFileOutputStream = null;
        try {
            destinationFileOutputStream = hdfsFileSystem.create(destinationFile, new Progressable() {
                @Override
                public void progress() {

                    System.out.print(".");

                }
            });
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileCreate.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Start copying byte by byte
        int aChar;
        try {
            while ((aChar = sourceFileInputStream.read()) != -1) {
                destinationFileOutputStream.writeByte((byte)aChar);    
            }
        } catch (IOException ex) {
            Logger.getLogger(ProgressableFileCopy.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        // Close the FileSystem
        try {
            sourceFileInputStream.close();
            destinationFileOutputStream.close();
            hdfsFileSystem.close();
            

        } catch (IOException ex) {
            Logger.getLogger(HdfsDirRead.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println("Read dir successfully");


    }
}
