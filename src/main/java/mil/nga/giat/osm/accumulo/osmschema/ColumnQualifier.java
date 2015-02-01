package mil.nga.giat.osm.accumulo.osmschema;

import com.google.common.base.Preconditions;

/**
 * Created by bennight on 1/31/2015.
 */
public class ColumnQualifier {
    public static final byte[] ID = "id".getBytes(Schema.CHARSET);
    public static final byte[] LATTITUDE = "lat".getBytes(Schema.CHARSET);
    public static final byte[] LONGITUDE = "lon".getBytes(Schema.CHARSET);
    public static final byte[] VERSION = "ver".getBytes(Schema.CHARSET);
    public static final byte[] TIMSESTAMP = "ts".getBytes(Schema.CHARSET);
    public static final byte[] CHANGESET = "cs".getBytes(Schema.CHARSET);
    public static final byte[] USER_TEXT = "ut".getBytes(Schema.CHARSET);
    public static final byte[] USER_ID = "uid".getBytes(Schema.CHARSET);
    public static final byte[] OSM_VISIBILITY = "vis".getBytes(Schema.CHARSET);
    public static final byte[] REFERENCES = "ref".getBytes(Schema.CHARSET);

    public static byte[] TAG_QUALIFIER(String tag){
        Preconditions.checkNotNull(tag);
        return tag.getBytes(Schema.CHARSET);
    }
}
