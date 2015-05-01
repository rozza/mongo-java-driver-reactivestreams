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

import com.mongodb.MongoException;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoIterable;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class MongoIterablePublisher<TResult> implements Publisher<TResult> {

    private final MongoIterable<TResult> mongoIterable;

    MongoIterablePublisher(final MongoIterable<TResult> mongoIterable) {
        this.mongoIterable = mongoIterable;
    }

    @Override
    public void subscribe(final Subscriber<? super TResult> s) {
        new AsyncBatchCursorSubscription(s).start();
    }

    private class AsyncBatchCursorSubscription extends SubscriptionSupport<TResult> {

        private final Lock booleanLock = new ReentrantLock(false);

        private boolean requestedBatchCursor;
        private boolean isReading;
        private boolean isProcessing;
        private boolean cursorCompleted;

        private final AtomicReference<AsyncBatchCursor<TResult>> batchCursor = new AtomicReference<AsyncBatchCursor<TResult>>();
        private final AtomicLong wanted = new AtomicLong();
        private final ConcurrentLinkedQueue<TResult> resultsQueue = new ConcurrentLinkedQueue<TResult>();

        public AsyncBatchCursorSubscription(final Subscriber<? super TResult> subscriber) {
            super(subscriber);
        }

        @Override
        protected void doRequest(final long n) {
            wanted.addAndGet(n);

            booleanLock.lock();
            boolean mustGetCursor = false;
            try {
                if (!requestedBatchCursor) {
                    requestedBatchCursor = true;
                    mustGetCursor = true;
                }
            } finally {
                booleanLock.unlock();
            }

            if (mustGetCursor) {
                getBatchCursor();
            } else if (batchCursor.get() != null) { // we have the batch cursor so start to process the resultsQueue
                processResultsQueue();
            }
        }

        @Override
        protected void handleCancel() {
            super.handleCancel();
            AsyncBatchCursor<TResult> cursor = batchCursor.get();
            if (cursor != null) {
                cursor.close();
            }
        }

        private void processResultsQueue() {
            booleanLock.lock();
            boolean mustProcess = false;
            try {
                if (!isProcessing) {
                    isProcessing = true;
                    mustProcess = true;
                }
            } finally {
                booleanLock.unlock();
            }

            if (mustProcess) {
                boolean mustGetAnotherBatch = false;
                long i = wanted.get();
                while (i > 0) {
                    TResult item = resultsQueue.poll();
                    if (item == null) {
                        mustGetAnotherBatch = true;
                        break;
                    } else {
                        onNext(item);
                        i = wanted.decrementAndGet();
                    }
                }

                if (cursorCompleted && resultsQueue.size() == 0) {
                    onComplete();
                }

                booleanLock.lock();
                try {
                    isProcessing = false;
                } finally {
                    booleanLock.unlock();
                }

                if (mustGetAnotherBatch) {
                    getNextBatch();
                }
            }
        }

        private void getNextBatch() {
            log("getNextBatch");

            booleanLock.lock();
            boolean mustRead = false;
            try {
                if (!isReading && !cursorCompleted) {
                    isReading = true;
                    mustRead = true;
                }
            } finally {
                booleanLock.unlock();
            }

            if (mustRead) {
                final AsyncBatchCursor<TResult> cursor = batchCursor.get();
                cursor.setBatchSize(getBatchSize());
                cursor.next(new SingleResultCallback<List<TResult>>() {
                    @Override
                    public void onResult(final List<TResult> result, final Throwable t) {
                        booleanLock.lock();
                        try {
                            isReading = false;
                            if (t == null && result == null | cursor.isClosed()) {
                                cursorCompleted = true;
                            }
                        } finally {
                            booleanLock.unlock();
                        }

                        if (t != null) {
                            onError(t);
                        } else {
                            if (result != null) {
                                resultsQueue.addAll(result);
                            }
                            processResultsQueue();
                        }
                    }
                });
            }
        }

        private void getBatchCursor() {
            mongoIterable.batchSize(getBatchSize());
            mongoIterable.batchCursor(new SingleResultCallback<AsyncBatchCursor<TResult>>() {
                @Override
                public void onResult(final AsyncBatchCursor<TResult> result, final Throwable t) {
                    if (t != null) {
                        onError(t);
                    } else if (result != null) {
                        batchCursor.set(result);
                        getNextBatch();
                    } else {
                        onError(new MongoException("Unexpected error, no AsyncBatchCursor returned from the MongoIterable."));
                    }
                }
            });
        }

        /**
         * Returns the batchSize to be used with the cursor.
         *
         * <p>Anything less than 2 would close the cursor so that is the minimum batchSize and `Integer.MAX_VALUE` is the maximum
         * batchSize.</p>
         *
         * @return the batchSize to use
         */
        private int getBatchSize() {
            long requested = wanted.get();
            if (requested <= 1) {
                return 2;
            } else if (requested < Integer.MAX_VALUE) {
                return (int) requested;
            } else {
                return Integer.MAX_VALUE;
            }
        }
    }

}
