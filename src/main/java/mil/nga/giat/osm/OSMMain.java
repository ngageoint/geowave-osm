package mil.nga.giat.osm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

/**
 * Entry point for OSM operations
 */
public class OSMMain {
    private final static Logger LOGGER = LoggerFactory.getLogger(OSMMain.class);

    public static void main(final String[] args) {
        final OSMCommandArgs osmArgs = new OSMCommandArgs();
        final JCommander cmd = new JCommander(osmArgs, args);

        if (args.length == 0){
            cmd.usage();
            System.exit(1);
        }

        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            LOGGER.error(ex.getLocalizedMessage(), ex);
            cmd.usage();
            System.exit(1);
        }



    }
}

