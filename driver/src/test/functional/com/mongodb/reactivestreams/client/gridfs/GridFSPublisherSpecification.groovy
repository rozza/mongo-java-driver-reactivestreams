/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.gridfs

import com.mongodb.MongoGridFSException
import com.mongodb.client.gridfs.model.GridFSFile
import com.mongodb.client.gridfs.model.GridFSUploadOptions
import com.mongodb.reactivestreams.client.MongoCollection
import com.mongodb.reactivestreams.client.MongoDatabase
import com.mongodb.reactivestreams.client.FunctionalSpecification
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.Document
import org.bson.UuidRepresentation
import org.bson.codecs.UuidCodec
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import spock.lang.Unroll

import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.WritableByteChannel
import java.security.SecureRandom

import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Updates.unset
import static com.mongodb.reactivestreams.client.Fixture.ObservableSubscriber
import static com.mongodb.reactivestreams.client.Fixture.createPublisher
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName
import static com.mongodb.reactivestreams.client.Fixture.getMongoClient
import static com.mongodb.reactivestreams.client.MongoClients.getDefaultCodecRegistry
import static java.util.concurrent.TimeUnit.SECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries

class GridFSPublisherSpecification extends FunctionalSpecification {
    protected MongoDatabase mongoDatabase;
    protected MongoCollection<GridFSFile> filesCollection;
    protected MongoCollection<Document> chunksCollection;
    protected GridFSBucket gridFSBucket;
    def singleChunkString = 'GridFS'
    def multiChunkString = singleChunkString.padLeft(1024 * 255 * 5)

    def setup() {
        mongoDatabase = getMongoClient().getDatabase(getDefaultDatabaseName())
        filesCollection = mongoDatabase.getCollection('fs.files', GridFSFile)
        chunksCollection = mongoDatabase.getCollection('fs.chunks')
        run(filesCollection.&drop)
        run(chunksCollection.&drop)
        gridFSBucket = GridFSBuckets.create(mongoDatabase)
    }

    def cleanup() {
        if (filesCollection != null) {
            run(filesCollection.&drop)
            run(chunksCollection.&drop)
        }
    }

    @Unroll
    def 'should round trip a #description'() {
        given:
        def content = multiChunk ? multiChunkString : singleChunkString
        def contentBytes = content as byte[]
        def expectedLength = contentBytes.length

        when:
        def fileId = run(gridFSBucket.&uploadFromPublisher, 'myFile', createPublisher(ByteBuffer.wrap(contentBytes)))

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == chunkCount

        when:
        def fileInfo = run(gridFSBucket.find().filter(eq('_id', fileId)).&first)

        then:
        fileInfo.getId().getValue() == fileId
        fileInfo.getChunkSize() == gridFSBucket.getChunkSizeBytes()
        fileInfo.getLength() == expectedLength
        fileInfo.getMetadata() == null

        when:
        def data = runAndCollect(gridFSBucket.&downloadToPublisher, fileId)

        then:
        concatByteBuffers(data) == contentBytes

        where:
        description           | multiChunk | chunkCount
        'a small file'        | false      | 1
        'a large file'        | true       | 5
    }

    def 'should round trip with small chunks'() {
        given:
        def contentSize = 1024 * 500
        def chunkSize = 10
        def contentBytes = new byte[contentSize / 2];
        new SecureRandom().nextBytes(contentBytes);
        def options = new GridFSUploadOptions().chunkSizeBytes(chunkSize)

        when:
        def fileId = run(gridFSBucket.&uploadFromPublisher, 'myFile', createPublisher(ByteBuffer.wrap(contentBytes),
                ByteBuffer.wrap(contentBytes)), options)

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == contentSize / chunkSize

        when:
        def data = runAndCollect(gridFSBucket.&downloadToPublisher, fileId)

        then:
        concatByteBuffers(data) == concatByteBuffers([ByteBuffer.wrap(contentBytes), ByteBuffer.wrap(contentBytes)])
    }

    def 'should round trip with data larger than the internal bufferSize'() {
        given:
        def contentSize = 1024 * 1024 * 5
        def chunkSize = 1024 * 1024
        def contentBytes = new byte[contentSize];
        new SecureRandom().nextBytes(contentBytes);
        def options = new GridFSUploadOptions().chunkSizeBytes(chunkSize)

        when:
        def fileId = run(gridFSBucket.&uploadFromPublisher, 'myFile', createPublisher(ByteBuffer.wrap(contentBytes)), options)

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == contentSize / chunkSize

        when:
        def data = runAndCollect(gridFSBucket.&downloadToPublisher, fileId)

        then:
        concatByteBuffers(data) == contentBytes
    }

    def 'should handle custom ids'() {
        def contentBytes = multiChunkString.getBytes()
        def fileId = new BsonString('myFile')

        when:
        run(gridFSBucket.&uploadFromPublisher, fileId, 'myFile', createPublisher(ByteBuffer.wrap(contentBytes)))
        def data = runAndCollect(gridFSBucket.&downloadToPublisher, fileId)

        then:
        concatByteBuffers(data) == contentBytes

        when:
        run(gridFSBucket.&rename, fileId, 'newName')
        data = runAndCollect(gridFSBucket.&downloadToPublisher, 'newName')

        then:
        concatByteBuffers(data) == contentBytes

        when:
        run(gridFSBucket.&delete, fileId)

        then:
        run(filesCollection.&countDocuments) == 0
        run(chunksCollection.&countDocuments) == 0
    }

    def 'should throw a chunk not found error when there are no chunks'() {
        given:
        def contentSize = 1024 * 1024
        def contentBytes = new byte[contentSize]
        new SecureRandom().nextBytes(contentBytes)

        when:
        def fileId = run(gridFSBucket.&uploadFromPublisher, 'myFile', createPublisher(ByteBuffer.wrap(contentBytes)))
        run(chunksCollection.&deleteMany, eq('files_id', fileId))
        run(gridFSBucket.&downloadToPublisher, fileId)

        then:
        thrown(MongoGridFSException)
    }

    def 'should round trip with a byteBuffer size of 4096'() {
        given:
        def contentSize = 1024 * 1024
        def contentBytes = new byte[contentSize]
        new SecureRandom().nextBytes(contentBytes)
        def options = new GridFSUploadOptions().chunkSizeBytes(1024)

        when:
        def fileId = run(gridFSBucket.&uploadFromPublisher, 'myFile', createPublisher(ByteBuffer.wrap(contentBytes)), options)

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == 1024

        when:
        def fileInfo = run(gridFSBucket.find().filter(eq('_id', fileId)).&first)

        then:
        fileInfo.getObjectId() == fileId
        fileInfo.getChunkSize() == 1024
        fileInfo.getLength() == contentSize
        fileInfo.getMetadata() == null

        when:
        def data = runAndCollect(gridFSBucket.downloadToPublisher(fileId).&bufferSizeBytes, 4096)

        then:
        data.size() == 256
        concatByteBuffers(data) == contentBytes
    }

    def 'should handle uploading publisher erroring'() {
        given:
        def errorMessage = 'Failure Propagated'
        def source = new Publisher<ByteBuffer>() {
            @Override
            void subscribe(final Subscriber<? super ByteBuffer> s) {
                s.onError(new IllegalArgumentException(errorMessage))
            }
        }
        when:
        run(gridFSBucket.&uploadFromPublisher, 'myFile', source)

        then:
        IllegalArgumentException ex = thrown()
        ex.getMessage() == errorMessage
    }

    def 'should use custom uploadOptions when uploading' () {
        given:
        def chunkSize = 20
        def metadata = new Document('archived', false)
        def options = new GridFSUploadOptions()
                .chunkSizeBytes(chunkSize)
                .metadata(metadata)
        def content = 'qwerty' * 1024
        def contentBytes = content as byte[]
        def expectedLength = contentBytes.length as Long
        def expectedNoChunks = Math.ceil((expectedLength as double) / chunkSize) as int


        when:
        def fileId = run(gridFSBucket.&uploadFromPublisher, 'myFile', createPublisher(ByteBuffer.wrap(contentBytes)), options)

        then:
        run(filesCollection.&countDocuments) == 1
        run(chunksCollection.&countDocuments) == expectedNoChunks

        when:
        def fileInfo = run(gridFSBucket.find().filter(eq('_id', fileId)).&first)

        then:
        fileInfo.getId().getValue() == fileId
        fileInfo.getChunkSize() == options.getChunkSizeBytes()
        fileInfo.getLength() == expectedLength
        fileInfo.getMetadata() == options.getMetadata()

        when:
        def data = runAndCollect(gridFSBucket.&downloadToPublisher, fileId)

        then:
        concatByteBuffers(data) == contentBytes
    }

    def 'should be able to open by name'() {
        given:
        def content = 'Hello GridFS'
        def contentBytes = content as byte[]
        def filename = 'myFile'
        run(gridFSBucket.&uploadFromPublisher, filename, createPublisher(ByteBuffer.wrap(contentBytes)))


        when:
        def data = runAndCollect(gridFSBucket.&downloadToPublisher, filename)

        then:
        concatByteBuffers(data) == contentBytes
    }

    def 'should be able to handle missing file'() {
        when:
        def filename = 'myFile'
        run(gridFSBucket.&downloadToPublisher, filename)

        then:
        thrown(MongoGridFSException)
    }

    def 'should create the indexes as expected'() {
        when:
        def filesIndexKey = Document.parse('{ filename: 1, uploadDate: 1 }')
        def chunksIndexKey = Document.parse('{ files_id: 1, n: 1 }')

        then:
        !runAndCollect(filesCollection.&listIndexes)*.get('key').contains(filesIndexKey)
        !runAndCollect(chunksCollection.&listIndexes)*.get('key').contains(chunksIndexKey)

        when:
        run(gridFSBucket.&uploadFromPublisher, 'myFile', createPublisher(ByteBuffer.wrap(multiChunkString.getBytes())))

        then:
        runAndCollect(filesCollection.&listIndexes)*.get('key').contains(Document.parse('{ filename: 1, uploadDate: 1 }'))
        runAndCollect(chunksCollection.&listIndexes)*.get('key').contains(Document.parse('{ files_id: 1, n: 1 }'))
    }

    def 'should not create indexes if the files collection is not empty'() {
        when:
        run(filesCollection.withDocumentClass(Document).&insertOne, new Document('filename', 'bad file'))
        def contentBytes = 'Hello GridFS' as byte[]

        then:
        runAndCollect(filesCollection.&listIndexes).size() == 1
        runAndCollect(chunksCollection.&listIndexes).size() == 0

        when:
        run(gridFSBucket.&uploadFromPublisher, 'myFile', createPublisher(ByteBuffer.wrap(contentBytes)))

        then:
        runAndCollect(filesCollection.&listIndexes).size() == 1
        runAndCollect(chunksCollection.&listIndexes).size() == 1
    }

    def 'should use the user provided codec registries for encoding / decoding data'() {
        given:
        def codecRegistry = fromRegistries(fromCodecs(new UuidCodec(UuidRepresentation.STANDARD)), getDefaultCodecRegistry())
        def database = getMongoClient().getDatabase(getDefaultDatabaseName()).withCodecRegistry(codecRegistry)
        def uuid = UUID.randomUUID()
        def fileMeta = new Document('uuid', uuid)
        def gridFSBucket = GridFSBuckets.create(database)

        when:
        def fileId = run(gridFSBucket.&uploadFromPublisher, 'myFile', createPublisher(ByteBuffer.wrap(multiChunkString.getBytes())),
                new GridFSUploadOptions().metadata(fileMeta))

        def file = run(gridFSBucket.find(new Document('_id', fileId)).&first)

        then:
        file.getMetadata() == fileMeta

        when:
        def fileAsDocument = run(filesCollection.find(BsonDocument).&first)

        then:
        fileAsDocument.getDocument('metadata').getBinary('uuid').getType() == 4 as byte
    }

    def 'should handle missing file name data when downloading #description'() {
        given:
        def contentBytes = multiChunkString.getBytes()

        when:
        def fileId = run(gridFSBucket.&uploadFromPublisher, 'myFile', createPublisher(ByteBuffer.wrap(contentBytes)))

        then:
        run(filesCollection.&countDocuments) == 1

        when:
        // Remove filename
        run(filesCollection.&updateOne, eq('_id', fileId), unset('filename'))
        def data = runAndCollect(gridFSBucket.&downloadToPublisher, fileId)

        then:
        concatByteBuffers(data) == contentBytes
    }

    def 'should cleanup when unsubscribing'() {
        when:
        def contentBytes = multiChunkString as byte[]

        then:
        run(filesCollection.&countDocuments) == 0

        when:
        def subscriber = new ObservableSubscriber()
        gridFSBucket.uploadFromPublisher('myFile', createPublisher(ByteBuffer.wrap(contentBytes), ByteBuffer.wrap(contentBytes)))
            .subscribe(subscriber)
        subscriber.getSubscription().request(1)

        then:
        !subscriber.isCompleted()

        then:
        run(filesCollection.&countDocuments) == 0
        tryMultipleTimes({ run(chunksCollection.&countDocuments) }, 5)

        then:
        subscriber.getSubscription().cancel()

        then:
        run(filesCollection.&countDocuments) == 0
        tryMultipleTimes({ run(chunksCollection.&countDocuments) }, 0)
    }

    def tryMultipleTimes(closure, expected) {
        def counter = 0
        while (counter < 5) {
            if (closure.call() == expected) {
                return true
            }
            sleep(500)
            counter++
        }
        assert closure.call() == expected
    }

    def run(operation, ... args) {
        def result = runAndCollect(operation, args)
        result != null ? result.get(0) : result
    }

    def runAndCollect(operation, ... args) {
        def subscriber = new ObservableSubscriber()
        operation.call(args).subscribe(subscriber)
        subscriber.get(30, SECONDS)
    }

    byte[] concatByteBuffers(List<ByteBuffer> buffers) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        WritableByteChannel channel = Channels.newChannel(outputStream)
        for (ByteBuffer buffer: buffers) {
            channel.write(buffer)
        }
        outputStream.close()
        channel.close()
        outputStream.toByteArray()
    }
}

