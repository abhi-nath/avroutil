package core;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import sample.avro.User;

import java.io.File;
import java.io.IOException;

public class SchemaSpecific {
    public static void main(String[] args) throws IOException {
        User user1 = new User();
        user1.setName("Rams");
        user1.setFavoriteColor("yellow");

        User user2 = new User("Ben", 235, "Yellow");

        // Construct via builder
        User user3 = User.newBuilder()
                .setName("Hugo")
                .setFavoriteColor("pink")
                .setFavoriteNumber(null)
                .build();

        File file = new File("specificUser.avro");
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();

        // Deserialize Users from disk
        DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<User>(file, userDatumReader);
            User user = null;
            while (dataFileReader.hasNext()) {
                // Reuse user object by passing it to next(). This saves us from
                // allocating and garbage collecting many objects for files with
                // many items.
                user = dataFileReader.next(user);
                System.out.println(user);
            }
        } finally {
            assert dataFileReader != null;
            dataFileReader.close();
        }
    }
}
