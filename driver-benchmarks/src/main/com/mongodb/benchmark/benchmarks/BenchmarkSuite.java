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
 *
 */

package com.mongodb.benchmark.benchmarks;

import com.mongodb.BasicDBObject;
import com.mongodb.benchmark.framework.Benchmark;
import com.mongodb.benchmark.framework.BenchmarkResult;
import com.mongodb.benchmark.framework.BenchmarkResultWriter;
import com.mongodb.benchmark.framework.BenchmarkRunner;
import com.mongodb.benchmark.framework.EvergreenBenchmarkResultWriter;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.List;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;

@SuppressWarnings({"rawtypes", "unchecked"})
public class BenchmarkSuite {

    public static final int NUM_WARMUP_ITERATIONS = 1;
    public static final int NUM_ITERATIONS = 100;

    private static final List<BenchmarkResultWriter> WRITERS = Arrays.<BenchmarkResultWriter>asList(
            new EvergreenBenchmarkResultWriter());

    public static void main(String[] args) throws Exception {
        Class clazz;
        if (args.length > 0) {
            clazz = Class.forName(args[0]);
        } else {
            clazz = Document.class;
        }

        runBenchmarks(clazz, getDefaultCodecRegistry().get(clazz), createIdRemover(clazz));

        for (BenchmarkResultWriter writer : WRITERS) {
            writer.close();
        }
    }

    private static <T extends Bson> void runBenchmarks(final Class<T> clazz, final Codec<T> codec, final IdRemover<T> idRemover)
            throws Exception {

        runBenchmark(new BsonEncodeDocumentBenchmark<T>("Flat", "extended_bson/flat_bson.json", codec));
        runBenchmark(new BsonEncodeDocumentBenchmark<T>("Deep", "extended_bson/deep_bson.json", codec));
        runBenchmark(new BsonEncodeDocumentBenchmark<T>("Full", "extended_bson/full_bson.json", codec));

        runBenchmark(new BsonDecodeDocumentBenchmark<T>("Flat", "extended_bson/flat_bson.json", codec));
        runBenchmark(new BsonDecodeDocumentBenchmark<T>("Deep", "extended_bson/deep_bson.json", codec));
        runBenchmark(new BsonDecodeDocumentBenchmark<T>("Full", "extended_bson/full_bson.json", codec));

        runBenchmark(new RunCommandBenchmark<T>(codec));
        runBenchmark(new FindOneBenchmark<T>("single_and_multi_document/tweet.json", clazz));

        runBenchmark(new InsertOneBenchmark<T>("Small", "./single_and_multi_document/small_doc.json", 10000, clazz, idRemover));
        runBenchmark(new InsertOneBenchmark<T>("Large", "./single_and_multi_document/large_doc.json", 10, clazz, idRemover));

        runBenchmark(new FindManyBenchmark<T>("single_and_multi_document/tweet.json", clazz));
        runBenchmark(new InsertManyBenchmark<T>("Small", "./single_and_multi_document/small_doc.json", 10000, clazz));
        runBenchmark(new InsertManyBenchmark<T>("Large", "./single_and_multi_document/large_doc.json", 10, clazz));

        runBenchmark(new GridFSUploadBenchmark("single_and_multi_document/gridfs_large.bin"));
        runBenchmark(new GridFSDownloadBenchmark("single_and_multi_document/gridfs_large.bin"));

        runBenchmark(new ImportBenchmark());
        runBenchmark(new ExportBenchmark());
        runBenchmark(new GridFSImportBenchmark());
        runBenchmark(new GridFSExportBenchmark());
    }

    private static void runBenchmark(final Benchmark benchmark) throws Exception {
        long startTime = System.currentTimeMillis();
        BenchmarkResult benchmarkResult = new BenchmarkRunner(benchmark, NUM_WARMUP_ITERATIONS, NUM_ITERATIONS, 60, 300 ).run();
        long endTime = System.currentTimeMillis();
        System.out.println(benchmarkResult.getName() + ": " + (endTime - startTime) / 1000.0);
        for (BenchmarkResultWriter writer : WRITERS) {
            writer.write(benchmarkResult);
        }
    }

    private static <T> IdRemover<T> createIdRemover(Class<T> clazz) {
        if (clazz.equals(BsonDocument.class)) {
            return (IdRemover<T>) new IdRemover<BsonDocument>() {
                public void removeId(final BsonDocument document) {
                    document.remove("_id");
                }
            };
        }

        if (clazz.equals(Document.class)) {
            return (IdRemover<T>) new IdRemover<Document>() {
                public void removeId(final Document document) {
                    document.remove("_id");
                }
            };
        }

        if (clazz.equals(BasicDBObject.class)) {
            return (IdRemover<T>) new IdRemover<BasicDBObject>() {
                public void removeId(final BasicDBObject document) {
                    document.remove("_id");
                }
            };
        }

        throw new UnsupportedOperationException("Pick a different class");
    }
}
