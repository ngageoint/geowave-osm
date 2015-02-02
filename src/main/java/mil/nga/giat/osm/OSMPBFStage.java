package mil.nga.giat.osm;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.openstreetmap.osmosis.osmbinary.Fileformat.BlobHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OSMPBFStage {

	
	private final Logger log = LoggerFactory.getLogger(OSMPBFStage.class);

	public static void main(String[] args) {

	}

	public void StageData(OSMCommandArgs args) throws IOException {
		final OSMCommandArgs arg = args;
		final Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://" + args.nameNode);
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

		Files.walkFileTree(Paths.get(args.ingestDirectory), new SimpleFileVisitor<java.nio.file.Path>(){
				@Override
				public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException{
					if (file.getFileName().toString().endsWith(arg.extension)) {
						stageToHadoop(file, arg.hdfsSequenceFile, conf);
					}
					return FileVisitResult.CONTINUE;
				}
		});
	}
	
	private void stageToHadoop(java.nio.file.Path localPBF, String sequenceFilePath, Configuration conf) {
		Path path = new Path(sequenceFilePath);
		LongWritable key = new LongWritable();
		BytesWritable value = new BytesWritable();
		SequenceFile.Writer writer = null;
		
		
		DataInputStream in = null;
		InputStream is = null;
	    try {  
	    	//File spec @ http://wiki.openstreetmap.org/wiki/PBF_Format
	    	//java.nio.file.Path localPBF = new java.nio.file.Path(localPBFInput);
	    	is = new FileInputStream(localPBF.toFile());
	    	in = new DataInputStream(is);
	    	writer = SequenceFile.createWriter(conf,  SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(key.getClass()), SequenceFile.Writer.valueClass(value.getClass()));
	    	long blockid = 0;
	    	while (in.available() > 0){
		        if (in.available() == 0) {break;}
			    int len = in.readInt();
			    byte[] blobHeader = new byte[len];
			    in.read(blobHeader);
			    BlobHeader h = BlobHeader.parseFrom(blobHeader);

				if (h.getType().equals("OSMData")){
			    	byte[] blob = new byte[h.getDatasize()];
			    	in.read(blob,0, h.getDatasize());
			    	key.set(blockid);
				    value.set(blob,0, blob.length);
				    writer.append(key, value);
				    blockid++;
			    }
			    else {
			    	in.skip(h.getDatasize());
			    }
		    }
	   } catch (IOException e) {
		   log.error(e.getLocalizedMessage());
		} finally {
			IOUtils.closeQuietly(is);
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(writer);
		}
	}
		
	


}
