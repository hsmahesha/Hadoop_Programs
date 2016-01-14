/**                                                                             
 * A toy java program which experiments the mahout kmeans clustering
 * algorithm.
 */                                                                             
                                                                                
/**                                                                             
 * command line usage: hadoop KMean                                                       
 */ 

// Java imports
import java.io.IOException;

// Hadoop imports
import org.apache.hadoop.conf.Configuration;                                    
import org.apache.hadoop.fs.FileSystem;                                         
import org.apache.hadoop.fs.Path;                                               
import org.apache.hadoop.io.SequenceFile.Reader;                                
import org.apache.hadoop.io.Writable;                                           
import org.apache.hadoop.util.ReflectionUtils;

// Mahout imports
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.clustering.kmeans.KMeansDriver; 
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

public class KMean {
  private static final String RAW_FILE = "../RAW_FILE";
  private static final String MAIN_TEST_DIR = "../TEST";
  private static final String INPUT_DIR = MAIN_TEST_DIR + "/" + "INPUT";
  private static final String OUTPUT_DIR = MAIN_TEST_DIR + "/" + "OUTPUT";
  private static final String SEED_DIR = MAIN_TEST_DIR + "/" + "SEED";
  private static final int K = 2;
  private static final double ConvergenceDelta = 0.5;
  private static final int MaxIterations = 10;
  private static final boolean RunClustering = true;
  private static final double ClusterClassificationThreshold = 0.0;
  private static final boolean RunSequential = false;
  private static final String InputSequenceVector = 
                 new String("org.apache.mahout.math.RandomAccessSparseVector");
  private static final EuclideanDistanceMeasure DistMeasure = new
                                                    EuclideanDistanceMeasure();

  private Configuration Conf;
  private FileSystem Hdfs;

  KMean() throws IOException {
    Conf = new Configuration();
    Hdfs = FileSystem.get(Conf);
  }

  private Path getFile(String pStr) throws IOException {
    Path pPath = new Path(pStr);
    if (!this.Hdfs.exists(pPath)) {
      System.out.println("Failed to locate a file " +
                         pPath.getName());
    }
    return pPath;
  }

  private Path createDir(String pStr) throws IOException {
    Path pPath = new Path(pStr);
    if (this.Hdfs.exists(pPath)) {
      this.Hdfs.delete(pPath, true);
    }
    if (!this.Hdfs.mkdirs(pPath)) {
      System.out.println("Failed to create a directory " +
                         pPath.getName());
    }
    return pPath;
  }

  private Path createFile(String pStr) throws IOException {
    Path pPath = new Path(pStr);
    if (!this.Hdfs.createNewFile(pPath)) {
      System.out.println("Failed to create a file " +
                         pPath.getName());
    }
    return pPath;
  }

  public static void main(String Args[]) throws IOException {
    KMean Obj = new KMean();

    Path rawFile = Obj.getFile(RAW_FILE);

    Obj.createDir(MAIN_TEST_DIR);
    Path InDir = new Path(INPUT_DIR);
    Path OutDir = Obj.createDir(OUTPUT_DIR);
    Path SeedDir = Obj.createDir(SEED_DIR);

    try {
      InputDriver.runJob(rawFile, InDir, InputSequenceVector);
    } catch (IOException e) { e.printStackTrace(); }
      catch (InterruptedException e) { e.printStackTrace(); }
      catch (ClassNotFoundException e) { e.printStackTrace(); }

    RandomSeedGenerator.buildRandom(Obj.Conf, InDir, SeedDir, K, DistMeasure);

    try {
      KMeansDriver.run(Obj.Conf, InDir, SeedDir, OutDir, ConvergenceDelta,
                     MaxIterations, RunClustering,
                     ClusterClassificationThreshold, RunSequential);
    } catch (IOException e) { }                                                 
      catch (InterruptedException e) { }                                        
      catch (ClassNotFoundException e) { } 

    Path oFile = Obj.getFile(OutDir + "/" + "clusteredPoints" + "/" +
                             "part-m-00000"); 
    Reader reader = new Reader(Obj.Conf, Reader.file(oFile),                        
                          Reader.bufferSize(4096), Reader.start(0));       
                                                                                          
    Writable key = (Writable) ReflectionUtils.newInstance(                     
                                      reader.getKeyClass(), Obj.Conf);         
    Writable value = (Writable) ReflectionUtils.newInstance(                   
                                      reader.getValueClass(), Obj.Conf);       

    System.out.printf("\n\n");                                          
    System.out.printf("===================================================\n"); 
    System.out.printf("Output\n");                                          
    System.out.printf("===================================================\n"); 
    while (reader.next(key, value)) {                                         
      System.out.printf("[cluster: %s]\t[%s]\n", key, value);                 
    }                                                                           
    System.out.printf("===================================================\n"); 

    reader.close();   
  }
}
