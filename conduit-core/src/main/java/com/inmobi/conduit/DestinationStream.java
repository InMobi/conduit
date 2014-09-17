/*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.inmobi.conduit;

public class DestinationStream {
  private final int retentionInHours;
  private final String name;
  private final Boolean isPrimary;
  private final Boolean isHCatEnabled;

  public DestinationStream(String name, int retentionInHours, Boolean isPrimary,
      Boolean isHCatEnabled) {
    this.name = name;
    this.retentionInHours = retentionInHours;
    this.isPrimary = isPrimary;
    this.isHCatEnabled = isHCatEnabled;
  }

  public Boolean isHCatEnabled() {
    return isHCatEnabled;
  }

  public boolean isPrimary() {
    return isPrimary;
  }

  public String getName() {
    return name;
  }

  public int getRetentionInHours() {
    return retentionInHours;
  }
}
