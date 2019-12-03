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
    public void subscribe(final Subscriber<? super Success> s) {
        s.onSubscribe(new GridFSUploadSubscription(s));
    }

    class GridFSUploadSubscription implements Subscription {
        private final Subscriber<? super Success> outerSubscriber;

        /* protected by `this` */
        private long requested = 0;
        private Action currentAction = Action.WAITING;
        private Subscription sourceSubscription;
        /* protected by `this` */

        GridFSUploadSubscription(final Subscriber<? super Success> outerSubscriber) {
            this.outerSubscriber = outerSubscriber;
        }

        private final Subscriber<ByteBuffer> sourceSubscriber = new Subscriber<ByteBuffer>() {
            @Override
            public void onSubscribe(final Subscription s) {
                synchronized (GridFSUploadSubscription.this) {
                    sourceSubscription = s;
                    currentAction = Action.WAITING;
                }
                tryProcess();
            }

            @Override
            public void onNext(final ByteBuffer byteBuffer) {
                gridFSUploadStream.write(byteBuffer).subscribe(new GridFSUploadStreamSubscriber(byteBuffer));
            }

            @Override
            public void onError(final Throwable t) {
                synchronized (GridFSUploadSubscription.this) {
                    currentAction = Action.FINISHED;
                }
                outerSubscriber.onError(t);
            }

            @Override
            public void onComplete() {
                synchronized (GridFSUploadSubscription.this) {
                    currentAction = Action.COMPLETE;
                }
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
                            if (currentAction != Action.COMPLETE && currentAction != Action.TERMINATE && currentAction != Action.FINISHED) {
                                currentAction = Action.WAITING;
                            }
                        }
                        tryProcess();
                    }
                }
            }
        };

        @Override
        public void request(final long n) {
            synchronized (this) {
                requested += n;
            }
            tryProcess();
        }

        @Override
        public void cancel() {
            terminate();
        }

        private void tryProcess() {
            NextStep nextStep;
            synchronized (this) {
                switch (currentAction) {
                    case WAITING:
                        if (requested == 0) {
                            nextStep = NextStep.DO_NOTHING;
                        } else if (sourceSubscription == null) {
                            nextStep = NextStep.SUBSCRIBE;
                            currentAction = Action.IN_PROGRESS;
                        } else {
                            requested--;
                            nextStep = NextStep.WRITE;
                            currentAction = Action.IN_PROGRESS;
                        }
                        break;
                    case COMPLETE:
                        nextStep = NextStep.COMPLETE;
                        currentAction = Action.FINISHED;
                        break;
                    case TERMINATE:
                        nextStep = NextStep.TERMINATE;
                        currentAction = Action.FINISHED;
                        break;
                    case IN_PROGRESS:
                    case FINISHED:
                    default:
                        nextStep = NextStep.DO_NOTHING;
                        break;
                }
            }

            switch (nextStep) {
                case SUBSCRIBE:
                    source.subscribe(sourceSubscriber);
                    break;
                case WRITE:
                    synchronized (this) {
                        sourceSubscription.request(1);
                    }
                    break;
                case COMPLETE:
                    gridFSUploadStream.close().subscribe(new Subscriber<Success>() {
                        @Override
                        public void onSubscribe(final Subscription s) {
                            s.request(1);
                        }

                        @Override
                        public void onNext(final Success success) {
                            outerSubscriber.onNext(success);
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
                    break;
                case TERMINATE:
                    gridFSUploadStream.abort().subscribe(new Subscriber<Success>() {
                        @Override
                        public void onSubscribe(final Subscription s) {
                            s.request(1);
                        }

                        @Override
                        public void onNext(final Success success) {
                        }

                        @Override
                        public void onError(final Throwable t) {
                        }

                        @Override
                        public void onComplete() {
                        }
                    });
                    break;
                case DO_NOTHING:
                default:
                    break;
            }
        }

        private void terminate() {
            synchronized (this) {
                currentAction = Action.TERMINATE;
            }
            tryProcess();
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

    enum Action {
        WAITING,
        IN_PROGRESS,
        TERMINATE,
        COMPLETE,
        FINISHED
    }

    enum NextStep {
        SUBSCRIBE,
        WRITE,
        COMPLETE,
        TERMINATE,
        DO_NOTHING
    }
}
