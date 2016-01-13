/**
 * A toy hadoop program which experiments the writing-to/reading-from hadoop
 * sequence file.
 */

/**
 * command line usage: 
 *    hadoop SequenceFileWriteAndRead /path/to/seq/file/to/be/writen/and/read
 */


// Java imports
import java.io.IOException;
import java.lang.Integer;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileWriteAndRead { 
   static final int SIZE = 10;
   static int[] IntKeys = new int[SIZE]; 
   static String[] StrValues = new String[SIZE];

   static void ConstructKeyValuePairs() {
      for (int i=0; i<SIZE; i++) {
         Integer c = new Integer(i+1);
         IntKeys[i] = c;
         StrValues[i] = "I am " + "\"" + c.toString() + "\"";
      }
   }

   public static void main(String[] args) throws IOException {
      // 0. create key and value array objects.
      SequenceFileWriteAndRead.ConstructKeyValuePairs();

      // 1. Create configuration object
      Configuration conf = new Configuration();

      // 2. Get hadoop file system object by passing configuration object
      FileSystem hdfs = FileSystem.get(conf);    

      // 3. Create a path object for hdfs file.
      String hStr = new String(args[0]); 
      Path hPath = new Path(hStr);  

      // 4. Check if hdfs file path object is created succssfully
      if (!hdfs.exists(hPath)) {
         System.out.println("Failed to locate hdfs file " + hPath.getName());
	 System.exit(0);
      }

      // 5. Create a key and value class objects for write.
      IntWritable key = new IntWritable();
      Text value = new Text();

      // 6. Construct Sequence file writer object.
      Writer writer = null;
      writer = SequenceFile.createWriter(conf,
		                         Writer.file(hPath),
					 Writer.keyClass(key.getClass()),
					 Writer.valueClass(value.getClass()));

      // 7. Append key/value pairs to sequence file
      for (int i=0; i<SequenceFileWriteAndRead.SIZE; i++) {
         int k = SequenceFileWriteAndRead.IntKeys[i];
	 String v = SequenceFileWriteAndRead.StrValues[i];
	 key.set(k);
	 value.set(v);
	 writer.append(key, value);
      }

      // 8. Close writer object
      writer.close();

      // 9. Construct sequence file reader object
      Reader reader = new Reader(conf, Reader.file(hPath), 
		                 Reader.bufferSize(4096), Reader.start(0));

      // 10. Create key and value class objects for read
      Writable key1 = (Writable) ReflectionUtils.newInstance(
		                          reader.getKeyClass(), conf);
      Writable value1 = (Writable) ReflectionUtils.newInstance(
		                          reader.getValueClass(),
					  conf);

      // 11. Read sequence file and print the reading to console
      while (reader.next(key1, value1)) {
         System.out.printf("[%s]\t%s\n", key1, value1);
      }

      // 12. Close reader object
      reader.close();
   }
}
