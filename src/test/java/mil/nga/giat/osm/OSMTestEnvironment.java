package mil.nga.giat.osm;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

/**
 *
 */
public class OSMTestEnvironment  {
    private final static Logger LOGGER = LoggerFactory.getLogger(OSMTestEnvironment.class);
    protected static final String TEST_RESOURCE_DIR = new File("./src/test/data/").getAbsolutePath().toString();
    protected static final String TEST_DATA_ZIP_RESOURCE_PATH = TEST_RESOURCE_DIR + "/" + "hangzhou_china.zip";
    protected static final String TEST_DATA_BASE_DIR = new File("./target/data/").getAbsoluteFile().toString();
    protected static final String HDFS_BASE = new File("./target/hdfs/").getAbsoluteFile().toString();
    protected static final String DEFAULT_JOB_TRACKER = "local";
    protected static final int MIN_INPUT_SPLITS = 2;
    protected static final int MAX_INPUT_SPLITS = 4;
    protected static Configuration CONF = new Configuration();
    protected static MiniDFSCluster HDFS_CLUSTER = null;
    protected static MiniMRYarnCluster YARN_MR_CLUSTER = null;
    protected static String YARN_CLUSTER_NAME = "YarnTestCluster";
    protected static final Object MUTEX = new Object();
    protected static Boolean INITIALIZED = false;
    protected static MiniAccumuloCluster ACCUMULO_CLUSTER = null;
    protected static final String ACCUMULO_BASE = new File("./target/accumulo/").getAbsolutePath().toString();
    protected static final String ACCUMULO_PASS = "changeit";
    protected static final String LOG_DIRECTORY = new File("./target/log/").getAbsolutePath().toString();
    protected static final String TEMP_DIRECTORY = new File("./target/temp/").getAbsolutePath().toString();



    public static String getLocalDataDirectory(){
        return TEST_DATA_BASE_DIR;
    }
    public static Configuration getConf(){
        synchronized (MUTEX) {
            if (INITIALIZED) {
                return CONF;
            }
        }
        return null;
    }

    public static String getAccumuloInstance(){
        synchronized (MUTEX){
            if (INITIALIZED){
                return ACCUMULO_CLUSTER.getInstanceName();
            }
        }
        return null;
    }

    public static String getAccumuloUser(){
        return "root";
    }

    public static String getAccumuloPass(){
        return ACCUMULO_PASS;
    }

    public static ClientConfiguration getAccumuloConfig(){
        synchronized (MUTEX) {
            if (INITIALIZED) {
                return ACCUMULO_CLUSTER.getClientConfig();
            }
        }
        return null;
    }



    public static ContentSummary getHDFSFileSummary(String filename) throws IOException {
        synchronized (MUTEX){
            if (INITIALIZED){
                org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(filename);
                FileSystem file = path.getFileSystem(CONF);
                ContentSummary cs = null;
                cs = file.getContentSummary(path);
                file.close();
                return cs;
            }
        }
        return null;
    }



    public static String getNameNode(){
        synchronized (MUTEX) {
            if (INITIALIZED) {
                return HDFS_CLUSTER.getNameNode().getNameNodeAddressHostPortString();
            } else {
                return "";

            }
        }
    }

    public static void Setup() throws URISyntaxException, ZipException, IOException, InterruptedException {

        synchronized (MUTEX) {
            if (INITIALIZED){
                return;
            }
            //zip data (pbf's, shapefiles, etc.)
            ZipFile data = new ZipFile(new File(TEST_DATA_ZIP_RESOURCE_PATH));
            data.extractAll(TEST_DATA_BASE_DIR);
           //System.setProperty("java.io.tmpdir", TEMP_DIRECTORY);
            //set CONF
            FileUtil.fullyDelete(new File(HDFS_BASE));
            CONF.set("hadoop.log.dir", LOG_DIRECTORY);
            CONF.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, HDFS_BASE);
            CONF.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 64);
            CONF.setClass(YarnConfiguration.RM_SCHEDULER, FifoScheduler.class, ResourceScheduler.class);
            String hadoopHome = System.getProperty("hadoop.home.dir");
            if (hadoopHome == null) {
                hadoopHome = System.getenv("HADOOP_HOME");
            }
            CONF.set("hadoop.home.dir", hadoopHome);

            //start DFS cluster
            MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(CONF);
            HDFS_CLUSTER = builder.build();

            //start YARN cluster
            YARN_MR_CLUSTER = new MiniMRYarnCluster(YARN_CLUSTER_NAME);
            YARN_MR_CLUSTER.init(CONF);
            //YARN_MR_CLUSTER.start();
            CONF = new YarnConfiguration(YARN_MR_CLUSTER.getConfig());

            //start MiniAccumuloCluster
            FileUtil.fullyDelete(new File(ACCUMULO_BASE));
            ACCUMULO_CLUSTER = new MiniAccumuloCluster(new File(ACCUMULO_BASE), ACCUMULO_PASS);




            ACCUMULO_CLUSTER.start();

            INITIALIZED = true;

        }
    }


    public static void Shutdown() throws IOException, InterruptedException {
        synchronized (MUTEX) {
            if (HDFS_CLUSTER != null && INITIALIZED) {
                HDFS_CLUSTER.shutdown(true);
               // YARN_MR_CLUSTER.stop();
                ACCUMULO_CLUSTER.stop();
                INITIALIZED = false;
            }

        }
    }



}
