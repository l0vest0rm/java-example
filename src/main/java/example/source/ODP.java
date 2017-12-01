/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package example.source;

import example.utils.ThrottledIterator;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

/**
 * Sample data for the {@link WindowJoin} example.
 */
@SuppressWarnings("serial")
public class ODP {
  public static final int TYPE_ORDER = 0;
  public static final int TYPE_DRIVER = 1;
  public static final int TYPE_PASSENGER = 2;
  private static final long[] ORDERS = {11,12,13,14,15};
  private static final long[] DRIVERS = {21,22,23,24,25};
  private static final long[] PASSENGERS = {31,32,33,34,35};

  public int type;
  public long orderid;
  public long driverid;
  public long passengerid;
  public long lastTs; //上次变化时间

  public ODP(int type, long orderid, long driverid, long passengerid){
    this.type = type;
    this.orderid = orderid;
    this.driverid = driverid;
    this.passengerid = passengerid;
  }

  public static ODP of(int type, long orderid, long driverid, long passengerid) {
    return new ODP(type, orderid, driverid, passengerid);
  }

  /**
   * Continuously generates (odp sample data).
   */
  public static class MixSource implements Iterator<ODP>, Serializable {

    private final Random rnd = new Random(hashCode());

    @Override
    public boolean hasNext() {
      return true;
    }

    @Override
    public ODP next() {
      int type = rnd.nextInt(3);
      switch (type){
        case TYPE_ORDER:
          boolean hasDriverid = rnd.nextBoolean();
          if (hasDriverid){
            return new ODP(type, ORDERS[rnd.nextInt(ORDERS.length)], DRIVERS[rnd.nextInt(DRIVERS.length)], PASSENGERS[rnd.nextInt(PASSENGERS.length)]);
          }else {
            return new ODP(type, ORDERS[rnd.nextInt(ORDERS.length)], 0, PASSENGERS[rnd.nextInt(PASSENGERS.length)]);
          }
        case TYPE_DRIVER:
          return new ODP(type, 0, DRIVERS[rnd.nextInt(DRIVERS.length)], 0);
        case TYPE_PASSENGER:
          return new ODP(type, 0, 0, PASSENGERS[rnd.nextInt(PASSENGERS.length)]);
        default:
          return null;
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    public static DataStream<ODP> getSource(StreamExecutionEnvironment env, long rate) {
      return env.fromCollection(new ThrottledIterator<>(new MixSource(), rate),
          TypeInformation.of(new TypeHint<ODP>(){}));
    }
  }
}
