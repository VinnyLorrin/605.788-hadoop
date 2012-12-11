package bdpuh.hadoopfilesystemapi;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

public class HdfsGzipCompression {

    public static void main(String args[]) {

        Configuration configuration = new Configuration();

        Path fileToRead = new Path("/x/y/movies.dat");

        // Get a copy of FileSystem Object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipCompression.class.getName()).log(Level.SEVERE, null, ex);
        }


        // Open a File for Reading
        FSDataInputStream fSDataInputStream = null;
        try {
            fSDataInputStream = fileSystem.open(fileToRead);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipCompression.class.getName()).log(Level.SEVERE, null, ex);
        }


        // Open a File for Writing .gz file
        System.out.println(fileToRead.getName());
        Path compressedFileToWrite = new Path(fileToRead.toUri().getPath() 
                + ".gz");
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = fileSystem.create(compressedFileToWrite);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipCompression.class.getName()).log(Level.SEVERE, null, ex);
        }
        
                
        // Get Compressed FileOutputStream
        CompressionCodec compressionCodec = new GzipCodec();
        CompressionOutputStream compressionOutputStream = null;
        try {
            compressionOutputStream =
                    compressionCodec.createOutputStream(fsDataOutputStream);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipCompression.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            IOUtils.copyBytes(fSDataInputStream, compressionOutputStream, configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipCompression.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Close the InputStream and FileSystem
        try {
            fSDataInputStream.close();
            fsDataOutputStream.close();
            compressionOutputStream.close();
            fileSystem.close();
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipCompression.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println("Compressed file successfully");


    }
}
