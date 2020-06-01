package com.cuteximi.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @program: orcdemo
 * @description: 简单例子：本文数据写入 ORC 文件
 * @author: TSL
 * @create: 2020-06-01 19:15
 **/
public class SimpleOrcWriter {

    public static void main(String[] args) throws IOException {
        // 配置
        Configuration conf = new Configuration();
        // 列信息
        TypeDescription schema = TypeDescription.fromString("struct<x:int,y:int>");
        // 创建一个 Writer 对象，设置路径、配置、列名、压缩格式等。
        Writer writer = OrcFile.createWriter(new Path("my-orc1.orc"),
                OrcFile.writerOptions(conf)
                            .setSchema(schema).compress(CompressionKind.SNAPPY));
        // 行批量写、默认最大行数 1024, VectorizedRowBatch.DEFAULT_SIZE
        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector x = (LongColumnVector) batch.cols[0];
        LongColumnVector y = (LongColumnVector) batch.cols[1];
        for(int r=0; r < 20; ++r) {
            int row = batch.size++;
            x.vector[row] = r;
            y.vector[row] = r * 3;
            // If the batch is full, write it out and start over.
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
    }
}
