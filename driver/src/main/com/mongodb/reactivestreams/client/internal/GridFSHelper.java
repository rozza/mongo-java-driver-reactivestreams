package com.mongodb.reactivestreams.client.internal;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.reactivestreams.client.Success;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;
import com.mongodb.reactivestreams.client.gridfs.AsyncOutputStream;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;

public final class GridFSHelper {

    public static com.mongodb.async.client.gridfs.AsyncInputStream toCallbackAsyncInputStream(final AsyncInputStream wrapped) {
        return new com.mongodb.async.client.gridfs.AsyncInputStream() {

            @Override
            public void read(final ByteBuffer dst, final SingleResultCallback<Integer> callback) {
                wrapped.read(dst).subscribe(new Subscriber<Integer>() {
                    private Integer result = null;
                    private Throwable error = null;

                    @Override
                    public void onSubscribe(final Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(final Integer integer) {
                        result = integer;
                    }

                    @Override
                    public void onError(final Throwable t) {
                        error = t;
                    }

                    @Override
                    public void onComplete() {
                        callback.onResult(result, error);
                    }
                });
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                wrapped.close().subscribe(new Subscriber<Success>() {
                    private Throwable error = null;

                    @Override
                    public void onSubscribe(final Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(final Success success) {
                    }

                    @Override
                    public void onError(final Throwable t) {
                        error = t;
                    }

                    @Override
                    public void onComplete() {
                        callback.onResult(null, error);
                    }
                });
            }
        };
    }

    public static com.mongodb.async.client.gridfs.AsyncOutputStream toCallbackAsyncOutputStream(final AsyncOutputStream wrapped) {
        return new com.mongodb.async.client.gridfs.AsyncOutputStream() {

            @Override
            public void write(final ByteBuffer src, final SingleResultCallback<Integer> callback) {
                wrapped.write(src).subscribe(new Subscriber<Integer>() {
                    private Integer result = null;
                    private Throwable error = null;

                    @Override
                    public void onSubscribe(final Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(final Integer integer) {
                        result = integer;
                    }

                    @Override
                    public void onError(final Throwable t) {
                        error = t;
                    }

                    @Override
                    public void onComplete() {
                        callback.onResult(result, error);
                    }
                });
            }

            @Override
            public void close(final SingleResultCallback<Void> callback) {
                wrapped.close().subscribe(new Subscriber<Success>() {
                    private Throwable error = null;

                    @Override
                    public void onSubscribe(final Subscription s) {
                        s.request(1);
                    }

                    @Override
                    public void onNext(final Success success) {
                    }

                    @Override
                    public void onError(final Throwable t) {
                        error = t;
                    }

                    @Override
                    public void onComplete() {
                        callback.onResult(null, error);
                    }
                });
            }
        };
    }

    private GridFSHelper() {
    }
}
