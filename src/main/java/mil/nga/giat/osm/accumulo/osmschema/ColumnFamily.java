package mil.nga.giat.osm.accumulo.osmschema;

/**
 * Created by bennight on 1/31/2015.
 */
public class ColumnFamily {
    public static final byte[] NODE = "n".getBytes(Schema.CHARSET);
    public static final byte[] NODE_TAG = "o".getBytes(Schema.CHARSET);
    public static final byte[] WAY = "w".getBytes(Schema.CHARSET);
    public static final byte[] WAY_TAG = "x".getBytes(Schema.CHARSET);
    public static final byte[] RELATION = "r".getBytes(Schema.CHARSET);
    public static final byte[] RELATION_TAG = "s".getBytes(Schema.CHARSET);
}
