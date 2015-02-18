package mil.nga.giat.osm.mapreduce.Ingest;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import mil.nga.giat.geowave.store.data.field.BasicWriter;
import mil.nga.giat.osm.accumulo.osmschema.Schema;
import mil.nga.giat.osm.types.TypeUtils;
import mil.nga.giat.osm.types.generated.LongArray;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Calendar;


public class OSMMapperBase<T> extends Mapper<AvroKey<T>, NullWritable, Text, Mutation> {

    private static final Logger log = LoggerFactory.getLogger(OSMMapperBase.class);
    protected final HashFunction _hf = Hashing.murmur3_128(1);

    protected final BasicWriter.LongWriter _longWriter = new BasicWriter.LongWriter();
    protected final BasicWriter.IntWriter _intWriter = new BasicWriter.IntWriter();
    protected final BasicWriter.StringWriter _stringWriter = new BasicWriter.StringWriter();
    protected final BasicWriter.DoubleWriter _doubleWriter = new BasicWriter.DoubleWriter();
    protected final BasicWriter.BooleanWriter _booleanWriter = new BasicWriter.BooleanWriter();
    protected final BasicWriter.CalendarWriter _calendarWriter = new BasicWriter.CalendarWriter();

    protected ColumnVisibility _visibility = new ColumnVisibility("public".getBytes(Schema.CHARSET));

    protected Text _tableName = new Text("OSM");

    protected byte[] getIdHash(long id) {
        return _hf.hashLong(id).asBytes();
    }

    protected void put(Mutation m, byte[] cf, byte[] cq, Long val) {
		if (val != null) {
			m.put(cf, cq, _visibility, _longWriter.writeField(val));
		}
    }

    protected void put(Mutation m, byte[] cf, byte[] cq, Integer val) {
		if (val != null) {
			m.put(cf, cq, _visibility, _intWriter.writeField(val));
		}
    }

    protected void put(Mutation m, byte[] cf, byte[] cq, Double val) {
		if (val != null) {
			m.put(cf, cq, _visibility, _doubleWriter.writeField(val));
		}
    }

    protected void put(Mutation m, byte[] cf, byte[] cq, String val) {
		if (val != null) {
			m.put(cf, cq, _visibility, _stringWriter.writeField(val));
		}
    }

	protected void put(Mutation m, byte[] cf, byte[] cq, CharSequence val) {
		if (val != null) {
			m.put(cf, cq, _visibility, _stringWriter.writeField(val.toString()));
		}
	}

    protected void put(Mutation m, byte[] cf, byte[] cq, Boolean val) {
		if (val != null) {
			m.put(cf, cq, _visibility, _booleanWriter.writeField(val));
		}
    }

    protected void put(Mutation m, byte[] cf, byte[] cq, Calendar val) {
		if (val != null) {
			m.put(cf, cq, _visibility, _calendarWriter.writeField(val));
		}
    }

    protected void put(Mutation m, byte[] cf, byte[] cq, LongArray val) {
        if (val != null){
            try {
                m.put(cf, cq, _visibility, TypeUtils.serializeLongArray(val));
            } catch (IOException e) {
                log.error("Unable to serialize LongArray instance", e);
            }
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        String tn = context.getConfiguration().get("tableName");
		if (tn != null && !tn.isEmpty()){
			_tableName.set(tn);
		}
		String visibility = context.getConfiguration().get("osmVisibility");
		if (visibility == null){
			visibility = "";
		}

        _visibility = new ColumnVisibility(visibility.getBytes(Schema.CHARSET));
    }


}
