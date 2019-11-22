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

import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.reactivestreams.client.Success;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;

@SuppressWarnings("deprecation")
public class GridFSDownloadPublisherImpl implements GridFSDownloadPublisher {
    private final com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream gridFSDownloadStream;
    private int bufferSizeBytes;

    GridFSDownloadPublisherImpl(final com.mongodb.reactivestreams.client.gridfs.GridFSDownloadStream gridFSDownloadStream) {
        this.gridFSDownloadStream = gridFSDownloadStream;
    }

    @Override
    public Publisher<GridFSFile> getGridFSFile() {
        return gridFSDownloadStream.getGridFSFile();
    }

    @Override
    public GridFSDownloadPublisher bufferSizeBytes(final int bufferSizeBytes) {
        this.bufferSizeBytes = bufferSizeBytes;
        return this;
    }

    @Override
    public void subscribe(final Subscriber<? super ByteBuffer> s) {
        s.onSubscribe(new GridFSDownloadSubscription(s));
    }

    class GridFSDownloadSubscription implements Subscription {
        private final Subscriber<? super ByteBuffer> outerSubscriber;

        /* protected by `this` */
        private GridFSFile gridFSFile;
        private boolean requestedData;
        private long sizeRead = 0;
        private long requested = 0;
        private int currentBatchSize = 0;
        private boolean isCompleted = false;
        private boolean isTerminated = false;
        /* protected by `this` */

        GridFSDownloadSubscription(final Subscriber<? super ByteBuffer> outerSubscriber) {
            this.outerSubscriber = outerSubscriber;
        }

        private final Subscriber<GridFSFile> gridFSFileSubscriber = new Subscriber<GridFSFile>() {
            @Override
            public void onSubscribe(final Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(final GridFSFile result) {
                synchronized (GridFSDownloadSubscription.this) {
                    gridFSFile = result;
                }
            }

            @Override
            public void onError(final Throwable t) {
                outerSubscriber.onError(t);
                terminate();
            }

            @Override
            public void onComplete() {
                synchronized (GridFSDownloadSubscription.this) {
                    requestedData = false;
                }
                requestMoreOrComplete();
            }
        };

        class GridFSDownloadStreamSubscriber implements Subscriber<Integer> {
            private final ByteBuffer byteBuffer;

            GridFSDownloadStreamSubscriber(final ByteBuffer byteBuffer) {
                this.byteBuffer = byteBuffer;
            }

            @Override
            public void onSubscribe(final Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(final Integer integer) {
                synchronized (GridFSDownloadSubscription.this) {
                    sizeRead += integer;
                }
            }

            @Override
            public void onError(final Throwable t) {
                terminate();
                outerSubscriber.onError(t);
            }

            @Override
            public void onComplete() {
                if (byteBuffer.remaining() > 0) {
                    gridFSDownloadStream.read(byteBuffer).subscribe(new GridFSDownloadStreamSubscriber(byteBuffer));
                } else {
                    synchronized (GridFSDownloadSubscription.this) {
                        requestedData = false;
                        if (sizeRead == gridFSFile.getLength()) {
                            isCompleted = true;
                        }
                    }
                    byteBuffer.flip();
                    outerSubscriber.onNext(byteBuffer);
                    requestMoreOrComplete();
                }
            }
        }

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
        }

        private void requestMoreOrComplete() {
            synchronized (this) {
                if (requested > 0 && !requestedData && !isTerminated && !isCompleted) {
                    requestedData = true;
                    if (gridFSFile == null) {
                        getGridFSFile().subscribe(gridFSFileSubscriber);
                    } else {
                        requested--;
                        int chunkSize = gridFSFile.getChunkSize();
                        long remaining = gridFSFile.getLength() - sizeRead;

                        if (remaining == 0) {
                            isCompleted = true;
                            requestedData = false;
                            requestMoreOrComplete();
                        } else {
                            int byteBufferSize = Math.max(chunkSize, bufferSizeBytes);
                            byteBufferSize =  Math.min(Long.valueOf(remaining).intValue(), byteBufferSize);
                            ByteBuffer byteBuffer = ByteBuffer.allocate(byteBufferSize);

                            if (currentBatchSize == 0) {
                                currentBatchSize = Math.max(byteBufferSize / chunkSize, 1);
                                gridFSDownloadStream.batchSize(currentBatchSize);
                            }
                            gridFSDownloadStream.read(byteBuffer).subscribe(new GridFSDownloadStreamSubscriber(byteBuffer));
                        }
                    }
                } else if (isCompleted && !requestedData && !isTerminated) {
                    isTerminated = true;
                    gridFSDownloadStream.close().subscribe(new Subscriber<Success>() {
                        @Override
                        public void onSubscribe(final Subscription s) {
                            s.request(1);
                        }

                        @Override
                        public void onNext(final Success success) {
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
}
