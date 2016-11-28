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

package com.mongodb.reactivestreams.client.gridfs.helpers;

import com.mongodb.Block;
import com.mongodb.MongoGridFSException;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.reactivestreams.client.Success;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream;
import com.mongodb.reactivestreams.client.internal.ObservableToPublisher;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static com.mongodb.async.client.Observables.observe;
import static com.mongodb.reactivestreams.client.internal.GridFSHelper.toCallbackAsyncInputStream;
import static com.mongodb.reactivestreams.client.internal.PublisherHelper.voidToSuccessCallback;
import static org.bson.assertions.Assertions.notNull;

/**
 * A general helper class that creates {@link AsyncInputStream} or {@link AsyncOutputStream} instances.
 *
 * Provides support for:
 * <ul>
 *     <li>{@code byte[]} - Converts byte arrays into Async Streams</li>
 *     <li>{@link ByteBuffer} - Converts ByteBuffers into Async Streams</li>
 *     <li>{@link InputStream} - Converts InputStreams into Async Streams (Note: InputStream implementations are blocking)</li>
 *     <li>{@link OutputStream} - Converts OutputStreams into Async Streams (Note: OutputStream implementations are blocking)</li>
 * </ul>
 *
 * @since 1.3
 */
public final class AsyncStreamHelper {

    /**
     * Converts a {@code byte[]} into a {@link AsyncInputStream}
     *
     * @param srcBytes the data source
     * @return the AsyncInputStream
     */
    public static AsyncInputStream toAsyncInputStream(final byte[] srcBytes) {
        return toAsyncInputStream(ByteBuffer.wrap(srcBytes));
    }

    /**
     * Converts a {@code byte[]} into a {@link AsyncOutputStream}
     *
     * @param dstBytes the data destination
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream toAsyncOutputStream(final byte[] dstBytes) {
        return toAsyncOutputStream(ByteBuffer.wrap(dstBytes));
    }

    /**
     * Converts a {@link ByteBuffer} into a {@link AsyncInputStream}
     *
     * @param srcByteBuffer the data source
     * @return the AsyncInputStream
     */
    public static AsyncInputStream toAsyncInputStream(final ByteBuffer srcByteBuffer) {
        final com.mongodb.async.client.gridfs.AsyncInputStream wrapper = com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper
                .toAsyncInputStream(srcByteBuffer);
        return new AsyncInputStream() {
            @Override
            public Publisher<Integer> read(final ByteBuffer dstByteBuffer) {
                return new ObservableToPublisher<Integer>(observe(new Block<SingleResultCallback<Integer>>() {
                    @Override
                    public void apply(final SingleResultCallback<Integer> callback) {
                        wrapper.read(srcByteBuffer, callback);
                    }
                }));
            }

            @Override
            public Publisher<Success> close() {
                return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final SingleResultCallback<Success> callback) {
                        wrapper.close(voidToSuccessCallback(callback));
                    }
                }));
            }
        };
    }

    /**
     * Converts a {@link ByteBuffer} into a {@link AsyncOutputStream}
     *
     * @param dstByteBuffer the data destination
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream toAsyncOutputStream(final ByteBuffer dstByteBuffer) {
        final com.mongodb.async.client.gridfs.AsyncOutputStream wrapper = com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper
                .toAsyncOutputStream(dstByteBuffer);
        return new AsyncOutputStream() {
            @Override
            public Publisher<Integer> write(final ByteBuffer src) {
                return new ObservableToPublisher<Integer>(observe(new Block<SingleResultCallback<Integer>>() {
                    @Override
                    public void apply(final SingleResultCallback<Integer> callback) {
                        wrapper.write(src, callback);
                    }
                }));
            }

            @Override
            public Publisher<Success> close() {
                return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final SingleResultCallback<Success> callback) {
                        wrapper.close(voidToSuccessCallback(callback));
                    }
                }));
            }
        };
    }

    /**
     * Converts a {@link InputStream} into a {@link AsyncInputStream}
     *
     * @param inputStream the InputStream
     * @return the AsyncInputStream
     */
    public static AsyncInputStream toAsyncInputStream(final InputStream inputStream) {
        final com.mongodb.async.client.gridfs.AsyncInputStream wrapper = com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper
                .toAsyncInputStream(inputStream);

        return new AsyncInputStream() {
            @Override
            public Publisher<Integer> read(final ByteBuffer dst) {
                return new ObservableToPublisher<Integer>(observe(new Block<SingleResultCallback<Integer>>() {
                    @Override
                    public void apply(final SingleResultCallback<Integer> callback) {
                        wrapper.read(dst, callback);
                    }
                }));
            }

            @Override
            public Publisher<Success> close() {
                return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final SingleResultCallback<Success> callback) {
                        wrapper.close(voidToSuccessCallback(callback));
                    }
                }));
            }
        };
    }

    /**
     * Converts a {@link OutputStream} into a {@link AsyncOutputStream}
     *
     * @param outputStream the OutputStream
     * @return the AsyncOutputStream
     */
    public static AsyncOutputStream toAsyncOutputStream(final OutputStream outputStream) {
        final com.mongodb.async.client.gridfs.AsyncOutputStream wrapper = com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper
                .toAsyncOutputStream(outputStream);
        return new AsyncOutputStream() {

            @Override
            public Publisher<Integer> write(final ByteBuffer src) {
                return new ObservableToPublisher<Integer>(observe(new Block<SingleResultCallback<Integer>>() {
                    @Override
                    public void apply(final SingleResultCallback<Integer> callback) {
                        wrapper.write(src, callback);
                    }
                }));
            }

            @Override
            public Publisher<Success> close() {
                return new ObservableToPublisher<Success>(observe(new Block<SingleResultCallback<Success>>() {
                    @Override
                    public void apply(final SingleResultCallback<Success> callback) {
                        wrapper.close(voidToSuccessCallback(callback));
                    }
                }));
            }
        };
    }

    private AsyncStreamHelper() {
    }
}
