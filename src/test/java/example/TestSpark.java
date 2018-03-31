package example;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

public class TestSpark {
  @Test
  public void testStuctedStreaming() throws Exception {
    SparkSession spark = SparkSession
        .builder()
        .appName("JavaStructuredNetworkWordCount")
        .getOrCreate();
  }
}
