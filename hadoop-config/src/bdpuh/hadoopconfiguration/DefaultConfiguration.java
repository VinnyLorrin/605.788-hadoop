package bdpuh.hadoopconfiguration;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

public class DefaultConfiguration {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();

        System.out.println ("\nPrinting Specific Configuration");
        System.out.println ("===============================");
        System.out.println ("fs.default.name = " + configuration.get("fs.default.name"));
        System.out.println ("dfs.name.dir = " + configuration.get("dfs.name.dir"));
        System.out.println("mapred.job.tracker = " + configuration.get("mapred.job.tracker"));
        
        System.out.println("\nPrinting all Configurations in a for loop");
        System.out.println("===========================");
        for (Map.Entry<String, String> entry : configuration) {
            System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
        }
        
        System.out.println("\nDumping all Configurations");
        System.out.println("===========================");
        Configuration.dumpConfiguration(configuration, new PrintWriter(System.out));
              
    }
}
