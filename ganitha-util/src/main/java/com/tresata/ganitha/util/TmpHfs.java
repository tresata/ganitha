package com.tresata.ganitha.util;

import java.util.UUID;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;

import cascading.flow.FlowProcess;
import cascading.tap.hadoop.Hfs;
import cascading.tap.Tap;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.scheme.hadoop.SequenceFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Temporary tap.
 */
@SuppressWarnings("serial")
public class TmpHfs extends Hfs {

    /**
     * SequenceFile Scheme that ties to the sourceFields to the presented sinkFields.
     * This way the fields written to the scheme can be read out again, which is handy for a temporary tap.
     */
    public static class TmpScheme extends SequenceFile {

        public TmpScheme(final Fields fields) {
            super(fields);
        }

        public TmpScheme() {
            this(Fields.UNKNOWN);
        }

        @Override
        public void presentSinkFields(final FlowProcess<JobConf> flowProcess, final Tap tap, final Fields fields) {
            presentSinkFieldsInternal(fields);
            setSourceFields(getSinkFields());
        }

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TmpHfs.class);

    public TmpHfs(final String name, final Fields fields, final Configuration conf) {
        super(new TmpScheme(fields), null, SinkMode.REPLACE);
        LOGGER.debug("constructing");
        final String path = new Path(CascadingUtils.getTempPath(conf),
                                     (name != null ? name : "tmp") + UUID.randomUUID().toString()).toString();
        LOGGER.info("path {}", path);
        setStringPath(path);
    }

    public TmpHfs(final Fields fields, final Configuration conf) {
        this(null, fields, conf);
    }

    public TmpHfs(final String name, final Configuration conf) {
        this(name, Fields.UNKNOWN, conf);
    }

    public TmpHfs(final Configuration conf) {
        this(null, Fields.UNKNOWN, conf);
    }

    public void delete(final Configuration conf) throws IOException {
        LOGGER.info("deleting {}", getPath());
        deleteResource(new JobConf(conf)); // throws IOException
    }

}
