package mil.nga.giat.osm.accumulo.osmschema;

import com.google.common.base.Preconditions;

/**
 *
 */
public class ColumnQualifier {
    public static final byte[] ID = "id".getBytes(Schema.CHARSET);
    public static final byte[] LATITUDE = "lat".getBytes(Schema.CHARSET);
    public static final byte[] LONGITUDE = "lon".getBytes(Schema.CHARSET);
    public static final byte[] VERSION = "ver".getBytes(Schema.CHARSET);
    public static final byte[] TIMESTAMP = "ts".getBytes(Schema.CHARSET);
    public static final byte[] CHANGESET = "cs".getBytes(Schema.CHARSET);
    public static final byte[] USER_TEXT = "ut".getBytes(Schema.CHARSET);
    public static final byte[] USER_ID = "uid".getBytes(Schema.CHARSET);
    public static final byte[] OSM_VISIBILITY = "vis".getBytes(Schema.CHARSET);
    public static final byte[] REFERENCES = "ref".getBytes(Schema.CHARSET);
    public static final String REFERENCE_MEMID_PREFIX = "refmem";
    public static final String REFERENCE_ROLEID_PREFIX = "refrol";
    public static final String REFERENCE_TYPE_PREFIX = "reftype";

    public static byte[] TAG_QUALIFIER(String tag){
        Preconditions.checkNotNull(tag);
        return tag.getBytes(Schema.CHARSET);
    }
}
