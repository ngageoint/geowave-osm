package mil.nga.giat.osm.mapreduce.Ingest;


import mil.nga.giat.osm.accumulo.osmschema.Schema;
import mil.nga.giat.osm.types.generated.Node;
import mil.nga.giat.osm.types.generated.Primitive;
import org.apache.accumulo.core.data.Mutation;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class OSMNodeMapper extends OSMMapperBase<Node>  {

    private static Logger LOGGER = LoggerFactory.getLogger(OSMNodeMapper.class);



    @Override
    public void map(AvroKey<Node> key, NullWritable value, Context context) throws IOException, InterruptedException {

        Node node = key.datum();
        Primitive p = node.getCommon();

       	Mutation m = new Mutation(getIdHash(p.getId()));
		//Mutation m = new Mutation(_longWriter.writeField(p.getId()));
		//Mutation m = new Mutation(p.getId().toString());

        put(m, Schema.CF.NODE, Schema.CQ.ID, p.getId());
        put(m, Schema.CF.NODE, Schema.CQ.LONGITUDE, node.getLongitude());
        put(m, Schema.CF.NODE, Schema.CQ.LATITUDE, node.getLatitude());


        if (!Long.valueOf(0).equals(p.getVersion())) {
            put(m, Schema.CF.NODE, Schema.CQ.VERSION, p.getVersion());
        }

        if (!Long.valueOf(0).equals(p.getTimestamp())) {
            put(m, Schema.CF.NODE, Schema.CQ.TIMESTAMP, p.getTimestamp());
        }

        if (!Long.valueOf(0).equals(p.getChangesetId())) {
            put(m, Schema.CF.NODE, Schema.CQ.CHANGESET, p.getChangesetId());
        }

        if (!Long.valueOf(0).equals(p.getUserId())) {
            put(m, Schema.CF.NODE, Schema.CQ.USER_ID, p.getUserId());
        }


        put(m, Schema.CF.NODE, Schema.CQ.USER_TEXT, p.getUserName());
        put(m, Schema.CF.NODE, Schema.CQ.OSM_VISIBILITY, p.getVisible());

        for (Map.Entry<CharSequence, CharSequence> kvp : p.getTags().entrySet()) {
            put(m, Schema.CF.NODE, kvp.getKey().toString().getBytes(Schema.CHARSET), kvp.getValue().toString());
        }
        context.write(_tableName, m);

    }
}
