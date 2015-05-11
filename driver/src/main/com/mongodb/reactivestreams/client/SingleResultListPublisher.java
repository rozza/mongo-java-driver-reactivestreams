/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.Observer;
import com.mongodb.async.client.Subscription;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;

import static com.mongodb.async.client.SubscriptionHelpers.subscribeToListBlock;

abstract class SingleResultListPublisher<TResult> implements Publisher<TResult> {

    @Override
    public void subscribe(final Subscriber<? super TResult> subscriber) {
        subscriber.onSubscribe(new AsyncSubscriptionAdapter<TResult>(new Function<Observer<TResult>, Subscription>() {
            @Override
            public Subscription apply(final Observer<TResult> observer) {
                return subscribeToListBlock(new Block<SingleResultCallback<List<TResult>>>() {
                    @Override
                    public void apply(final SingleResultCallback<List<TResult>> callback) {
                        execute(callback);
                    }
                }, observer);
            }
        }, subscriber));
    }

    abstract void execute(SingleResultCallback<List<TResult>> callback);

    SingleResultListPublisher() {
    }
}
