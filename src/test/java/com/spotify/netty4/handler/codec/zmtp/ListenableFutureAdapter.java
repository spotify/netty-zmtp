/*
 * Copyright (c) 2012-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.netty4.handler.codec.zmtp;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

public class ListenableFutureAdapter<V> extends AbstractFuture<V> implements GenericFutureListener<Future<V>> {

  static <T> ListenableFuture<T> listenable(final Future<T> future) {
    final ListenableFutureAdapter<T> adapter = new ListenableFutureAdapter<T>();
    future.addListener(adapter);
    return adapter;
  }

  @Override
  public void operationComplete(final Future<V> future) throws Exception {
    if (future.isSuccess()) {
      set(future.getNow());
    } else {
      setException(future.cause());
    }
  }
}
