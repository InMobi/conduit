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
package com.inmobi.databus;

/*
 * Interface to store and retrieve checkpoints.
 */
public interface CheckpointProvider {

  /*
   * Read the checkpoint for the given key. If no checkpoint is found, null
   * is returned.
   */
  byte[] read(String key);

  /*
   * Stores the checkpoint for the given key.
   */
  void checkpoint(String key, byte[] checkpoint);

  /*
   * Closes the provider.
   */
  void close();
}
