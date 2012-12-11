/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package bdpuh.hadoopfilesystemapi;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author hadoop
 */
public class HdfsFileUpdateForAppend {
    
    public static void main (String args[]) {
        Configuration configuration = new Configuration();

        Path fileToAppendUpdate = new Path("/records.txt");        

        // Get a copy of File System Object
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileUpdateForAppend.class.getName()).log(Level.SEVERE, null, ex);
        }


        // Open a File for append update
        FSDataOutputStream fSDataOutputStream = null;
        try {
            fSDataOutputStream = fileSystem.append(fileToAppendUpdate);
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileUpdateForAppend.class.getName()).log(Level.SEVERE, null, ex);
        }


        
        String record4 = "This is record 004\n";
        String record5 = "This is record 005\n";
        String record6 = "This is record 006\n";
        try {
            // Write String records to the file
             fSDataOutputStream.writeChars(record4);
             fSDataOutputStream.writeChars(record5);
             fSDataOutputStream.writeChars(record6);
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileUpdateForAppend.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            fSDataOutputStream.close();
        } catch (IOException ex) {
            Logger.getLogger(HdfsFileUpdateForAppend.class.getName()).log(Level.SEVERE, null, ex);
        }


        System.out.println ("Append Update file successfully");
        
    }

    
}
