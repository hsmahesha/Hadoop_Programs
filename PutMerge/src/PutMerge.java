/**
 * A toy hadoop program which experiments the merging of content from many local
 * linux file system files to a single hadoop hdfs file system file.
 */

/**
 * command line usage: 
 *    hadoop PutMerge /path/to/local/dir /path/to/hdfs/file
 */


// Java imports
import java.io.IOException;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class PutMerge {
   public static void main(String[] args) throws IOException {
      // 1. Create configuration object
      Configuration conf = new Configuration();

      // 2. Get local linux file sytem object by passing configuration object
      FileSystem local = FileSystem.getLocal(conf); 

      // 3. Get hadoop file system object by passing configuration object
      FileSystem hdfs = FileSystem.get(conf);    

      // 4. Create a path object for local directory.
      String lStr = new String(args[0]);
      Path lPath = new Path(lStr);

      // 5. Check if local directory path object is created successfully
      if (!local.exists(lPath)) {
         System.out.println("Failed to locate local directory " +
			    lPath.getName());
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

      // 8. Get the list of all files in local input directory
      FileStatus[] lDir = local.listStatus(lPath);

      // 9. Open hdfs output file
      FSDataOutputStream hFile = hdfs.create(hPath);

      // 10. Traverse each local file in the local input directory, open it, 
      //     copy its content to hdfs file, and then close the local file.
      for (int i=0; i<lDir.length; i++) {
	 System.out.printf("%s", "Copying from " +
			   lDir[i].getPath().getName() + "\n");
         FSDataInputStream lFile = local.open(lDir[i].getPath());
         byte buffer[] = new byte[256];                                      
         int bytesRead = 0;                                                  
         while ((bytesRead = lFile.read(buffer)) > 0) {                         
            hFile.write(buffer, 0, bytesRead);                                 
         }
	 lFile.close();
      }
	
      // 11. Close hdfs files
      hFile.close();
   }
}
