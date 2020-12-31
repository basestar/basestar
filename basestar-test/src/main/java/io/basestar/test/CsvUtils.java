package io.basestar.test;

import io.basestar.test.annotation.PretendGenerated;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@PretendGenerated
public class CsvUtils {

    public static List<Map<String, String>> read(final Class<?> resourceClass, final String resource) throws IOException {

        try(final InputStream is = resourceClass.getResourceAsStream(resource)) {
            return read(is);
        }
    }

    public static List<Map<String, String>> read(final InputStream is) throws IOException {

        try(final InputStreamReader reader = new InputStreamReader(is)) {
            return read(reader);
        }
    }

    public static List<Map<String, String>> read(final Reader reader) throws IOException {

        return CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withTrim(true)
                .parse(reader).getRecords()
                .stream().map(CSVRecord::toMap)
                .collect(Collectors.toList());
    }
}
