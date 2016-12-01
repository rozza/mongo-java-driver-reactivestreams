/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.gridfs.helpers

import com.mongodb.async.client.gridfs.helpers.AsyncStreamHelper as WrappedAsyncStreamHelper
import org.reactivestreams.Subscriber
import spock.lang.Specification

import java.nio.ByteBuffer

class AsyncStreamHelperSpecification extends Specification {

    def subscriber = Stub(Subscriber) {
        onSubscribe(_) >> { args -> args[0].request(100) }
    }

    def 'should have the same methods as the wrapped AsyncStreamHelper'() {
        given:
        def wrapped = WrappedAsyncStreamHelper.methods*.name.sort()
        def local = AsyncStreamHelper.methods*.name.sort()

        expect:
        wrapped == local
    }

    def 'should call the underlying InputStream methods'() {

        given:
        def inputStream = Mock(InputStream)
        def byteBuffer = ByteBuffer.allocate(10)
        def asyncInputStream = AsyncStreamHelper.toAsyncInputStream(inputStream)

        when:
        asyncInputStream.read(byteBuffer).subscribe(subscriber)

        then:
        1 * inputStream.read(_)

        when:
        asyncInputStream.close().subscribe(subscriber)

        then:
        1 * inputStream.close()
    }

    def 'should call the underlying OutputStream methods'() {

        given:
        def outputStream = Mock(OutputStream)
        def byteBuffer = ByteBuffer.wrap(new byte[10])
        def asyncOutputStream = AsyncStreamHelper.toAsyncOutputStream(outputStream)

        when:
        asyncOutputStream.write(byteBuffer).subscribe(subscriber)

        then:
        1 * outputStream.write(_)

        when:
        asyncOutputStream.close().subscribe(subscriber)

        then:
        1 * outputStream.close()
    }

}
