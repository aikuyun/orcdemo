package com.cuteximi.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;

import java.io.IOException;

/**
 * @program: orcdemo
 * @description: 简单例子：读取 ORC 文件
 * @author: TSL
 * @create: 2020-06-01 19:15
 **/
public class SimpleOrcReader {

    public static void main(String[] args) throws IOException {
        // 配置
        Configuration conf = new Configuration();
        Reader reader = OrcFile.createReader(new Path("my-orc1.orc"),
                OrcFile.readerOptions(conf));
        // 拿到所有行
        RecordReader rows = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch();
        // 列处理器
        LongColumnVector x = (LongColumnVector) batch.cols[0];
        LongColumnVector y = (LongColumnVector) batch.cols[1];
        // 读也是默认为 1024 行为一个 batch
        while (rows.nextBatch(batch)) {
            // 打印到控制台
            for(int r=0; r < batch.size; ++r) {
                System.out.println(x.vector[r]+"\t"+y.vector[r]);
            }
        }
        rows.close();

    }


}
