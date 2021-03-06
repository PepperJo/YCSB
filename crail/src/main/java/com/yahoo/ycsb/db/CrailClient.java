/**
 * Copyright (c) 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import com.ibm.crail.CrailBufferedInputStream;
import com.ibm.crail.CrailBufferedOutputStream;
import com.ibm.crail.CrailFS;
import com.ibm.crail.CrailKeyValue;
import com.ibm.crail.CrailLocationClass;
import com.ibm.crail.CrailNodeType;
import com.ibm.crail.CrailStorageClass;
import com.ibm.crail.conf.CrailConfiguration;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

/**
 * Crail binding for <a href="http://www.crail.io/">Crail</a>.
 */
public class CrailClient extends DB {
  private CrailFS client;
  private boolean ready;
  private Random random;
  private long startTime;
  private long endTime;

  @Override
  public void init() throws DBException {
    super.init();
    try {
      CrailConfiguration crailConf = new CrailConfiguration();
      this.client = CrailFS.newInstance(crailConf);
      this.random = new Random();
    } catch(Exception e){
      throw new DBException(e);
    }
    try {
      client.create("usertable", CrailNodeType.TABLE, CrailStorageClass.DEFAULT,
        CrailLocationClass.DEFAULT).get().syncDir();
    } catch(Exception e){
      ready = true;
    }
    this.startTime = System.nanoTime();
    System.out.println("YCSB/Crail init, v1");
  }

  @Override
  public void cleanup() throws DBException {
    try {
      this.endTime = System.nanoTime();
      long runTime = (endTime - startTime) / 1000000;
      System.out.println("runTime " + runTime);
      client.close();
    } catch(Exception e){
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      String path = table + "/" + key;
      CrailKeyValue file = client.lookup(path).get().asKeyValue();
      CrailBufferedInputStream stream = file.getBufferedInputStream(1024);
      while(stream.available() < Integer.BYTES);
      int fieldKeyLength = stream.readInt();
      while(stream.available() < fieldKeyLength);
      byte[] fieldKey = new byte[fieldKeyLength];
      int res = stream.read(fieldKey);
      if (res != fieldKey.length){
        stream.close();
        return Status.ERROR;
      }
      while(stream.available() < Integer.BYTES);
      int fieldValueLength = stream.readInt();
      while(stream.available() < fieldValueLength);
      byte[] fieldValue = new byte[fieldValueLength];
      res = stream.read(fieldValue);
      if (res != fieldValue.length){
        stream.close();
        return Status.ERROR;
      }
      result.put(new String(fieldKey), new ByteArrayByteIterator(fieldValue));

      stream.close();
      return Status.OK;
    } catch(Exception e){
      e.printStackTrace();
      return new Status("read error", "reading exception");
    }
  }

  @Override
  public Status scan(String table, String startKey, int recordCount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    try {
      String path = table + "/" + key;
      CrailKeyValue file = client.create(path, CrailNodeType.KEYVALUE, CrailStorageClass.DEFAULT,
          CrailLocationClass.DEFAULT).get().asKeyValue();
      CrailBufferedOutputStream stream = file.getBufferedOutputStream(1024);
      for (Entry<String, ByteIterator> entry : values.entrySet()){
        byte[] fieldKey = entry.getKey().getBytes();
        int fieldKeyLength = fieldKey.length;
        byte[] fieldValue = entry.getValue().toArray();
        int fieldValueLength = fieldValue.length;
        stream.writeInt(fieldKeyLength);
        stream.write(fieldKey);
        stream.writeInt(fieldValueLength);
        stream.write(fieldValue);
      }
      file.syncDir();
      stream.close();
    } catch(Exception e){
      System.out.println(e.getMessage());
      e.printStackTrace();
      return Status.ERROR;
    }
    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      String path = table + "/" + key;
      client.delete(path, false).get().syncDir();
    } catch(Exception e){
      return Status.ERROR;
    }
    return Status.OK;
  }



}
