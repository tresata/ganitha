package com.tresata.ganitha.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide static utility/helper functions.
 */
public final class CascadingUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(CascadingUtils.class);

    private CascadingUtils() {
        // this is a utility class that cannot be instantiated
        throw new IllegalStateException();
    }

    public static Path getTempPath(final Configuration conf) {
        // Preconditions.checkNotNull(conf, "conf cannot be null");
        if (conf.get("cascading.tmp.dir") != null)
            return new Path(conf.get("cascading.tmp.dir"));
        else
            return new Path(conf.get("hadoop.tmp.dir"));
    }

    public static void setTempPath(final Configuration conf, final Path path) {
        // Preconditions.checkNotNull(conf, "conf cannot be null");
        conf.set("cascading.tmp.dir", path.toString());
    }

}
