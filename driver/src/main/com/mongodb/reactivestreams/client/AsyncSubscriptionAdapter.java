/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client;

import com.mongodb.Function;
import com.mongodb.async.client.Observer;
import com.mongodb.async.client.Subscription;
import org.reactivestreams.Subscriber;

class AsyncSubscriptionAdapter<TResult> implements org.reactivestreams.Subscription  {
    private final Subscription subscription;
    private final Subscriber<? super TResult> subscriber;

    AsyncSubscriptionAdapter(final Function<Observer<TResult>, Subscription> subscriptionProvider,
                             final Subscriber<? super TResult> subscriber) {
        this.subscriber = subscriber;
        this.subscription = subscriptionProvider.apply(getObserver());
    }

    @Override
    public void request(final long n) {
        if (!subscription.isUnsubscribed() && n < 1) {
            subscriber.onError(new IllegalArgumentException("3.9 While the Subscription is not cancelled, "
                    + "Subscription.request(long n) MUST throw a java.lang.IllegalArgumentException if the argument is <= 0."));
        } else {
            subscription.request(n);
        }
    }

    @Override
    public void cancel() {
        subscription.unsubscribe();
    }

    private Observer<TResult> getObserver() {
        return new Observer<TResult>() {

            @Override
            public void onSubscribe(final Subscription s) {
            }

            @Override
            public void onNext(final TResult tResult) {
                subscriber.onNext(tResult);
            }

            @Override
            public void onError(final Throwable t) {
                subscriber.onError(t);
            }

            @Override
            public void onComplete() {
                subscriber.onComplete();
            }
        };
    }
}
