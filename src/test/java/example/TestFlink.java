/*
 * Copyright 2017 example authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"): you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http: *www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

// Created by xuning on 2017/6/20

package example;

import example.source.ODP;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TestFlink {
  private static final Logger LOG = LoggerFactory.getLogger(TestFlink.class);
  private static final long ODP_CHANGE_TS = 30*1000;

  @Test
  public void testMixSource() throws Exception {
    // parse the parameters
    final ParameterTool params = ParameterTool.fromArgs(new String[]{});
    final long windowSize = params.getLong("windowSize", 2000);
    final long rate = params.getLong("rate", 3L);

    System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);
    System.out.println("To customize example, use: WindowJoin [--windowSize <window-size-in-millis>] [--rate <elements-per-second>]");

    // obtain execution environment, run this example in "ingestion time"
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    // create the data sources for both grades and salaries
    DataStream<ODP> stream = ODP.MixSource.getSource(env, rate);

    class ODPFlatMapFunction implements FlatMapFunction<ODP, ODP>{
      private boolean isKeyByDriverid;
      private Map<Long, ODP> odpMap;
      private long key;
      private ODP odp;

      public ODPFlatMapFunction(boolean isKeyByDriverid) {
        odpMap = new HashMap<>();
        this.isKeyByDriverid = isKeyByDriverid;
      }

      @Override
      public void flatMap(ODP value, Collector<ODP> out) throws Exception{
        if (isKeyByDriverid){
          key = value.driverid;
        }else {
          key = value.passengerid;
        }

        if (key == 0){
          return;
        }

        boolean changed = false;
        long curTs = System.currentTimeMillis();
        if (odpMap.containsKey(key)){
          odp = odpMap.get(key);
          if (value.orderid != 0 && value.orderid != odp.orderid){
            odp.orderid = value.orderid;
            changed = true;
          }
          if (value.driverid != 0 && value.driverid != odp.driverid){
            odp.driverid = value.driverid;
            changed = true;
          }
          if (value.passengerid != 0 && value.passengerid != odp.passengerid){
            odp.passengerid = value.passengerid;
            changed = true;
          }

          if (changed && odp.lastTs + ODP_CHANGE_TS < curTs){
            odp.lastTs = curTs;
            out.collect(odp);
          }
          return;
        }else {
          switch (value.type){
            case ODP.TYPE_ORDER:
            case ODP.TYPE_PASSENGER:
              value.lastTs = curTs;
              odpMap.put(key, value);
              out.collect(odp);
              break;
            default:
              return;
          }
        }

      }
    }

    class PassengeridKeySelector implements KeySelector<ODP, Long> {
      @Override
      public Long getKey(ODP value) throws Exception{
        return value.passengerid;
      }
    }

    ODPFlatMapFunction odpFlatMap = new ODPFlatMapFunction(false);

    stream.keyBy(new PassengeridKeySelector())
        .flatMap(odpFlatMap).addSink(new SinkFunction<ODP>() {
      @Override
      public void invoke(ODP value) throws Exception {
        if (value == null){
          return;
        }

        LOG.info("odp,type:{},order:{},driverid:{},passengerid:{}",
            value.type,
            value.orderid,
            value.driverid,
            value.passengerid);
      }
    });


    // execute program
    env.execute("ODP Example");
  }
}
