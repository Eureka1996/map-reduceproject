package com.wufuqiang.mr.mappers;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @ author wufuqiang
 **/
public class DistributedCacheMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    private HashMap<String,String> pdMap = new HashMap<String,String>();
    private Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //缓存小表
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())){
            String[] fields = line.split("\t");
            pdMap.put(fields[0],fields[1]);
        }
        IOUtils.closeStream(reader);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String pid = fields[1];
        String pname = pdMap.get(pid);
        line = line + "\t" + pname;
        k.set(line);
        context.write(k,NullWritable.get());
    }
}
