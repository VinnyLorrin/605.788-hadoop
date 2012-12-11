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
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;

public class HdfsGzipDecompression {

    public static void main(String args[]) {

        Configuration configuration = new Configuration();

        Path fileToRead = new Path("/x/y/movies.dat.gz");

        // Get a copy of FileSystem Object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipDecompression.class.getName()).log(Level.SEVERE, null, ex);
        }


        // Open a File for Reading
        FSDataInputStream fSDataInputStream = null;
        try {
            fSDataInputStream = fileSystem.open(fileToRead);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipDecompression.class.getName()).log(Level.SEVERE, null, ex);
        }

        if (configuration == null) 
            System.out.println("null");
        else 
            System.out.println("all good");

        // Open a File for Writing regular file
        System.out.println(fileToRead.getName());
        Path uncompressedFileToWrite = new Path(fileToRead.toUri().getPath() 
                + ".ugz");// Remove .ugz
        FSDataOutputStream fsDataOutputStream = null;
        try {
            fsDataOutputStream = fileSystem.create(uncompressedFileToWrite);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipDecompression.class.getName()).log(Level.SEVERE, null, ex);
        }
        
                
        // Get Deccompressed InputStream
        CompressionCodec compressionCodec = new GzipCodec();
        if (compressionCodec == null)
            System.out.println ("codec is null");
        else
            System.out.println("codec is not null");
        
       CompressionInputStream compressionInputStream = null;
        try {
            compressionInputStream =
                    compressionCodec.createInputStream(fSDataInputStream);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipDecompression.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            IOUtils.copyBytes(compressionInputStream, fsDataOutputStream, configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipDecompression.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Close the InputStream and FileSystem
        try {
            fSDataInputStream.close();
            fsDataOutputStream.close();
            compressionInputStream.close();
            fileSystem.close();
        } catch (IOException ex) {
            Logger.getLogger(HdfsGzipDecompression.class.getName()).log(Level.SEVERE, null, ex);
        }

        System.out.println("Decompressed file successfully");


    }
}
