/**                                                                             
 * A toy java program which experiments the mahouts kmeans clustering
 * algorithm.
 */                                                                             
                                                                                
/**                                                                             
 * command line usage:                                                          
 *    hadoop MahoutKMeanCluster /path/to/in/file /path/to/init/centroid/file
 *                              /path/to/out/dir
 */ 

// Java imports
import java.io.IOException;
import java.util.List;                                                          
import java.util.ArrayList; 

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

// Mahout imports
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.clustering.kmeans.Kluster; 
import org.apache.mahout.clustering.kmeans.KMeansDriver; 
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

public class MahoutKMeanCluster {
  private final double[][] RawPoints = { {1, 1}, {2, 1}, {1, 2},
                                         {2, 2}, {3, 3}, {8, 8},       
                                         {9, 8}, {8, 9}, {9, 9} };

  private List<Vector> ConvertRawPointsToMahoutVector() {
    List<Vector> VectorList = new ArrayList<Vector>();
    for (int i=0; i<this.RawPoints.length; i++) {
      double[] Point = RawPoints[i];
      Vector Vec = new RandomAccessSparseVector(Point.length);
      Vec.assign(Point);
      VectorList.add(Vec);
    }
    return VectorList;
  }

  private Path WriteVectorsToSequenceFile(Configuration Conf, 
                                          FileSystem Hdfs,
                                          String Arg0,
                                          List<Vector> VectorList) 
                                                           throws IOException {
    // Create a path object for input hdfs sequence file.                                 
    String iStr = new String(Arg0);                                        
    Path iPath = new Path(iStr);

    // create new input sequence file
    if (!Hdfs.createNewFile(iPath)) {
      System.out.println("Failed to create new input sequence file " +
                         iPath.getName());   
      System.exit(0);                                                        
    }

    // Create a key and value class objects for write.                     
    IntWritable key = new IntWritable();                                      
    VectorWritable value = new VectorWritable(); 

    // Construct Sequence file writer object.                              
    Writer writer = null;                                                     
    writer = SequenceFile.createWriter(Conf,                                  
                                       Writer.file(iPath),                    
                                       Writer.keyClass(key.getClass()),       
                                       Writer.valueClass(value.getClass())); 

    // Append key/value pairs to sequence file
    int i = 1;
    for (Vector vec: VectorList) {
      key.set(i++); 
      value.set(vec);                                                          
      writer.append(key, value);                                             
    }

    // Close writer object 
    writer.close();

    return iPath;
  }

  private Path WriteInitialCentroidsToSequenceFile(int K, 
                                            Configuration Conf, 
                                            FileSystem Hdfs, String Arg1,
                                            List<Vector> VectorList)
                                                           throws IOException {
    // Create a path object for initial centroid sequence file.                                 
    String cStr = new String(Arg1);                                        
    Path cPath = new Path(cStr);

    // Create new init centroid file              
    if (!Hdfs.createNewFile(cPath)) {                                                
      System.out.println("Failed to create new initial centroid sequence file "
                        + cPath.getName());   
      System.exit(0);                                                        
    }

    // Create a key and value class objects for write.                     
    Text key = new Text();                                      
    Kluster value = new Kluster(); 

    // Construct Sequence file writer object.                              
    Writer writer = null;                                                     
    writer = SequenceFile.createWriter(Conf,                                  
                                      Writer.file(cPath),                    
                                      Writer.keyClass(key.getClass()),       
                                      Writer.valueClass(value.getClass())); 

    // Append key/value pairs to sequence file
    for (int i = 0; i < K; i++) {                                   
      Vector vec = VectorList.get(i);                            
      Kluster cluster = new Kluster(vec, i, new EuclideanDistanceMeasure());
      writer.append(new Text(cluster.getIdentifier()), cluster);
    } 

    // Close writer object 
    writer.close();

    return cPath;
  }

  private Path RunKMeanClusterer(Configuration Conf, FileSystem Hdfs,
                                 String Arg2, Path iPath,
                                 Path cPath) throws IOException {
    // Create a path object for output directory                                 
    String oStr = new String(Arg2);                                        
    Path oPath = new Path(oStr);

    // Create new output directory              
    if (!Hdfs.mkdirs(oPath)) {                                                
      System.out.println("Failed to create new output directory "
                        + cPath.getName());   
      System.exit(0);                                                        
    }

    try {                                                         
      KMeansDriver.run(Conf, iPath.getParent(), cPath.getParent(), 
                       oPath, 0.001, 10, true, 0, false);                    
    } catch (IOException io) { }                                    
      catch (InterruptedException ie) { }                           
      catch (ClassNotFoundException cnf) { } 

    return oPath;
  }

  private void PrintClusteredPoints(Configuration Conf, FileSystem Hdfs,
                                    Path oPath) throws IOException {
    // Create a path object for output sequence file 
    String fStr = oPath.getName() + "/clusteredPoints/part-m-00000";
    Path fPath = new Path(fStr);

    // Check if output path object is created succssfully               
    if (!Hdfs.exists(fPath)) {                                                
      System.out.println("Failed to locate output sequence file "
                        + fPath.getName());   
      System.exit(0);                                                        
    }

    // Construct sequence file reader object                               
    Reader reader = new Reader(Conf, Reader.file(fPath),                      
                               Reader.bufferSize(4096), Reader.start(0));

    // Create key and value class objects for read                        
    Writable key1 = (Writable) ReflectionUtils.newInstance(                   
                                           reader.getKeyClass(), Conf);          
    Writable value1 = (Writable) ReflectionUtils.newInstance(                 
                                           reader.getValueClass(), Conf);       

    // Read sequence file and print the reading to console
    System.out.printf("\n\nOutput\n");
    System.out.printf("===================================================\n");
    while (reader.next(key1, value1)) {                                       
      System.out.printf("[cluster: %s]\t[%s]\n", key1, value1);                         
    }
    System.out.printf("===================================================\n");

    // Close reader object                                                
    reader.close();
  }

  public static void main(String Args[]) throws IOException {
    // Set the value of k for k-means cluster
    int K = 2;

    // create configuration object                                         
    Configuration Conf = new Configuration();

    // Get hadoop file system object by passing configuration object       
    FileSystem Hdfs = FileSystem.get(Conf);

    // Construct self object
    MahoutKMeanCluster Obj = new MahoutKMeanCluster();

    // Get mahout vectors from raw points
    List<Vector> VectorList = Obj.ConvertRawPointsToMahoutVector();

    // Write vectors to sequence file
    Path iPath = Obj.WriteVectorsToSequenceFile(Conf, Hdfs, Args[0],
                                                VectorList);

    // Write initial k centroids to sequence file
    Path cPath = Obj.WriteInitialCentroidsToSequenceFile(K, Conf, Hdfs, 
                                                         Args[1], VectorList); 

    // Run kmeans clustering algorithm
    Path oPath = Obj.RunKMeanClusterer(Conf, Hdfs, Args[2], iPath, cPath);

    // Print clustered points
    Obj.PrintClusteredPoints(Conf, Hdfs, oPath);
  }
}
