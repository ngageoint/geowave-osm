package mil.nga.giat.osm.accumulo.osmschema;



import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class Schema {
    public static Charset CHARSET = StandardCharsets.UTF_8;

    public static final ColumnFamily CF = new ColumnFamily();
    public static final ColumnQualifier CQ = new ColumnQualifier();


}
