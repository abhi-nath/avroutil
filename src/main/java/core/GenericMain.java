package core;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class GenericMain {
    public static void main(String[] args) throws IOException {
        GenericMain genericMain = new GenericMain();
        InputStream inputStream = genericMain.getClass().getResourceAsStream("/avro/user.avsc");
        Schema schema = new Parser().parse(inputStream);

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Michael");
        user1.put("favorite_color", "red");
        user1.put("favorite_number", 876);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "Caroline");
        user2.put("favorite_number", 76);

        // serialize user1 and user 2 to disk
        File user_avro_file;
        try {
            user_avro_file =new File("users.avro");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, user_avro_file);
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.close();

        //Deserialize users from disk
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<>(user_avro_file, datumReader);
            GenericRecord gen_user = null;
            while (dataFileReader.hasNext()) {
                gen_user = dataFileReader.next(gen_user);
                System.out.println(gen_user);
            }

        }
        finally {
            if (dataFileReader != null) {
                dataFileReader.close();
            }
        }
    }
}
