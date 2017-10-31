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
import java.util.Set;
import java.util.Vector;

import com.ibm.crail.CrailFS;
import com.ibm.crail.conf.CrailConfiguration;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

/**
 * Crail binding for <a href="http://www.crail.io/">Crail</a>.
 */
public class CrailClient extends DB {
  private CrailFS client;

  @Override
  public void init() throws DBException {
    super.init();
    try {
      CrailConfiguration crailConf = new CrailConfiguration();
      this.client = CrailFS.newInstance(crailConf);
    } catch(Exception e){
      throw new DBException(e);  
    }
  }
  
  @Override
  public void cleanup() throws DBException {
    try {
      client.close();
    } catch(Exception e){
      throw new DBException(e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    return null;
  }
  
  @Override
  public Status scan(String table, String startKey, int recordCount, Set<String> fields, 
      Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return null;
  }  

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return null;
  }

  @Override
  public Status delete(String table, String key) {
    return null;
  }



}
