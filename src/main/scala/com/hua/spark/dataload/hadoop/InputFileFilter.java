package com.hua.spark.dataload.hadoop;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * 文件过滤器
 */
public class InputFileFilter implements PathFilter, Configurable {

    private Configuration conf;
    private String filterRegex = ".*";

    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(InputFileFilter.class);

    /**
     * 文件过滤正则key
     */
    public static final String INPUT_FILE_FILTER = "input.file.filter";

    @Override
    public boolean accept(Path path) {

        try {
            if (path.toString().matches(filterRegex) || path.getFileSystem(conf).isDirectory(path)) {
                return true;
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return false;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        filterRegex = conf.get(INPUT_FILE_FILTER, filterRegex);
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

}
