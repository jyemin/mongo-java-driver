package tour;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class MultipleCursorsTour {
    public static void main(String[] args) {
        try (MongoClient client = MongoClients.create()) {
            MongoCollection<Document> collection = client.getDatabase("test").getCollection("facets");
            collection.drop();
            collection.insertOne(new Document("x", 2).append("age", 12).append("gender", "M"));
            collection.insertOne(new Document("x", 1).append("age", 15).append("gender", "M"));
            collection.insertOne(new Document("x", 1).append("age", 16).append("gender", "M"));
            collection.insertOne(new Document("x", 1).append("age", 15).append("gender", "M"));
            collection.insertOne(new Document("x", 1).append("age", 16).append("gender", "F"));
            collection.insertOne(new Document("x", 1).append("age", 17).append("gender", "F"));
            collection.insertOne(new Document("x", 1).append("age", 19).append("gender", "F"));
            collection.insertOne(new Document("x", 1).append("age", 19).append("gender", "F"));

            List<List<Document>> facetResults = collection
                    .aggregate(singletonList(match(eq("x", 1))))
                    .facets(asList(asList(group("$age")), asList(group("$gender"))))
                    .into((Supplier<List<Document>>) ArrayList::new);
            for (List<Document> result : facetResults) {
                System.out.println(result);
            }
        }
    }
}
