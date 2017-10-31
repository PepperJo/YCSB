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

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;

/**
 * Crail binding for <a href="http://www.crail.io/">Crail</a>.
 */
public class CrailClient extends DB {
  @Override
  public Status delete(String arg0, String arg1) {
    return null;
  }

  @Override
  public Status insert(String arg0, String arg1, Map<String, ByteIterator> arg2) {
    return null;
  }

  @Override
  public Status read(String arg0, String arg1, Set<String> arg2, Map<String, ByteIterator> arg3) {
    return null;
  }

  @Override
  public Status scan(String arg0, String arg1, int arg2, Set<String> arg3, Vector<HashMap<String, ByteIterator>> arg4) {
    return null;
  }

  @Override
  public Status update(String arg0, String arg1, Map<String, ByteIterator> arg2) {
    return null;
  }
}
