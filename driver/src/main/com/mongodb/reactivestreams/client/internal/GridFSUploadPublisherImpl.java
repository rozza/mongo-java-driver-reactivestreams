/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.reactivestreams.client.internal;

import com.mongodb.reactivestreams.client.Success;
import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;

@SuppressWarnings("deprecation")
public class GridFSUploadPublisherImpl implements GridFSUploadPublisher<Success> {
    private final com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream gridFSUploadStream;
    private final Publisher<ByteBuffer> source;

    GridFSUploadPublisherImpl(final com.mongodb.reactivestreams.client.gridfs.GridFSUploadStream gridFSUploadStream,
                              final Publisher<ByteBuffer> source) {
        this.gridFSUploadStream = gridFSUploadStream;
        this.source = source;
    }

    @Override
    public ObjectId getObjectId() {
        return gridFSUploadStream.getObjectId();
    }

    @Override
    public BsonValue getId() {
        return gridFSUploadStream.getId();
    }

    @Override
    public Publisher<Success> abort() {
        return gridFSUploadStream.abort();
    }

    @Override
    public void subscribe(final Subscriber<? super Success> s) {
        s.onSubscribe(new GridFSUploadSubscription(s));
    }

    class GridFSUploadSubscription implements Subscription {
        private final Subscriber<? super Success> outerSubscriber;

        /* protected by `this` */
        private boolean requestedData;
        private long requested = 0;
        private boolean isCompleted = false;
        private boolean isTerminated = false;
        private boolean subscribed = false;
        private Subscription sourceSubscription;
        /* protected by `this` */

        GridFSUploadSubscription(final Subscriber<? super Success> outerSubscriber) {
            this.outerSubscriber = outerSubscriber;
        }

        private final Subscriber<ByteBuffer> sourceSubscriber = new Subscriber<ByteBuffer>() {
            @Override
            public void onSubscribe(final Subscription s) {
                synchronized (GridFSUploadSubscription.this) {
                    subscribed = true;
                    sourceSubscription = s;
                }
                s.request(1);
            }

            @Override
            public void onNext(final ByteBuffer byteBuffer) {
                gridFSUploadStream.write(byteBuffer).subscribe(new GridFSUploadStreamSubscriber(byteBuffer));
            }

            @Override
            public void onError(final Throwable t) {
                outerSubscriber.onError(t);
            }

            @Override
            public void onComplete() {
                synchronized (GridFSUploadSubscription.this) {
                    isCompleted = true;
                }
                requestMoreOrComplete();
            }


            class GridFSUploadStreamSubscriber implements Subscriber<Integer> {
                private final ByteBuffer byteBuffer;

                GridFSUploadStreamSubscriber(final ByteBuffer byteBuffer) {
                    this.byteBuffer = byteBuffer;
                }

                @Override
                public void onSubscribe(final Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(final Integer integer) {
                }

                @Override
                public void onError(final Throwable t) {
                    terminate();
                    outerSubscriber.onError(t);
                }

                @Override
                public void onComplete() {
                    if (byteBuffer.remaining() > 0) {
                        sourceSubscriber.onNext(byteBuffer);
                    } else {
                        synchronized (GridFSUploadSubscription.this) {
                            requestedData = false;
                        }
                        requestMoreOrComplete();
                    }
                }
            }
        };

        @Override
        public void request(final long n) {
            synchronized (this) {
                requested += n;
            }
            requestMoreOrComplete();
        }

        @Override
        public void cancel() {
            terminate();
            gridFSUploadStream.abort().subscribe(new Subscriber<Success>() {
                @Override
                public void onSubscribe(final Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(final Success success) {
                    // Do nothing
                }

                @Override
                public void onError(final Throwable t) {
                    outerSubscriber.onError(t);
                }

                @Override
                public void onComplete() {
                    // Do nothing
                }
            });
        }

        private void requestMoreOrComplete() {
            synchronized (this) {
                if (requested > 0 && !requestedData && !isTerminated && !isCompleted) {
                    requestedData = true;
                    requested--;
                    if (!subscribed) {
                        source.subscribe(sourceSubscriber);
                    } else {
                        sourceSubscription.request(1);
                    }
                } else if (isCompleted && !requestedData && !isTerminated) {
                    isTerminated = true;
                    gridFSUploadStream.close().subscribe(new Subscriber<Success>() {
                        @Override
                        public void onSubscribe(final Subscription s) {
                            s.request(1);
                        }

                        @Override
                        public void onNext(final Success success) {
                            outerSubscriber.onNext(Success.SUCCESS);
                        }

                        @Override
                        public void onError(final Throwable t) {
                            outerSubscriber.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            outerSubscriber.onComplete();
                        }
                    });
                }
            }
        }

        private void terminate() {
            synchronized (this) {
                isTerminated = true;
            }
        }
    }

    GridFSUploadPublisher<ObjectId> withObjectId() {
        final GridFSUploadPublisherImpl wrapped = this;
        return new GridFSUploadPublisher<ObjectId>() {

            @Override
            public ObjectId getObjectId() {
                return wrapped.getObjectId();
            }

            @Override
            public BsonValue getId() {
                return wrapped.getId();
            }

            @Override
            public Publisher<Success> abort() {
                return wrapped.abort();
            }

            @Override
            public void subscribe(final Subscriber<? super ObjectId> objectIdSub) {
                wrapped.subscribe(new Subscriber<Success>() {
                    @Override
                    public void onSubscribe(final Subscription s) {
                        objectIdSub.onSubscribe(s);
                    }

                    @Override
                    public void onNext(final Success success) {
                        objectIdSub.onNext(getObjectId());
                    }

                    @Override
                    public void onError(final Throwable t) {
                        objectIdSub.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        objectIdSub.onComplete();
                    }
                });
            }
        };
    }
}
