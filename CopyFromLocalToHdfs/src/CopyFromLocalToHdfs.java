/**
 * A toy hadoop program which experiments the copying of content from local
 * linux file system file to hadoop hdfs file system file.
 */

/**
 * command line usage: 
 *    hadoop CopyFromLocalToHdfs /path/to/local/file /path/to/hdfs/file
 */


// Java imports
import java.io.IOException;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.

public class CopyFromLocalToHdfs {
   public static void main(String[] args) throws IOException {
      // 1. Create configuration object
      Configuration conf = new Configuration();

      // 2. Get local linux file sytem object by passing configuration object
      FileSystem local = FileSystem.getLocal(conf); 

      local.

      // 3. Get hadoop file system object by passing configuration object
      FileSystem hdfs = FileSystem.get(conf);    

      // 4. Create a path object for local file.
      String lStr = new String(args[0]);
      Path lPath = new Path(lStr);

      // 5. Check if local file path object is created successfully
      if (!local.exists(lPath)) {
         System.out.println("Failed to locate local file " + lPath.getName());
	 System.exit(0);
      }

      // 6. Create a path object for hdfs file.
      String hStr = new String(args[1]); 
      Path hPath = new Path(hStr);  

      // 7. Check if hdfs file path object is created succssfully
      if (!hdfs.exists(hPath)) {
         System.out.println("Failed to locate hdfs file " + hPath.getName());
	 System.exit(0);
      }

      // 8. Open local input file
      FSDataInputStream lFile = local.open(lPath);

      // 9. Open hdfs output file
      FSDataOutputStream hFile = hdfs.create(hPath);

      // 10. Copy from local to hdfs
      byte buffer[] = new byte[256];                                      
      int bytesRead = 0;                                                  
      while ((bytesRead = lFile.read(buffer)) > 0) {                         
         hFile.write(buffer, 0, bytesRead);                                 
      }           
	
      // 11. Close both local and hdfs files
      lFile.close();
      hFile.close();
   }
}
