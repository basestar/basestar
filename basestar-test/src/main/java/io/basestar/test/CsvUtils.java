package io.basestar.test;

import lombok.Generated;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Generated
public class CsvUtils {

    public static CSVFormat FORMAT = CSVFormat.DEFAULT;

    public static List<Map<String, String>> read(final Class<?> resourceClass, final String resource) throws IOException {

        try (final InputStream is = resourceClass.getResourceAsStream(resource)) {
            return read(is);
        }
    }

    public static List<Map<String, String>> read(final InputStream is) throws IOException {

        try (final InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8)) {
            return read(reader);
        }
    }

    public static List<Map<String, String>> read(final Reader reader) throws IOException {

        return FORMAT.withFirstRecordAsHeader()
                .withTrim(true).parse(reader).getRecords()
                .stream().map(CSVRecord::toMap)
                .collect(Collectors.toList());
    }

    public static void write(final OutputStream os, final List<? extends Map<String, ?>> records) throws IOException {

        try (final OutputStreamWriter writer = new OutputStreamWriter(os)) {
            write(writer, records);
        }
    }

    public static void write(final Writer writer, final List<? extends Map<String, ?>> records) throws IOException {

        final Set<String> names = new LinkedHashSet<>();
        records.forEach(record -> names.addAll(record.keySet()));

        try (final CSVPrinter printer = FORMAT.withHeader(names.toArray(new String[0])).print(writer)) {
            for (final Map<String, ?> record : records) {
                printer.printRecord(names.stream().map(record::get).toArray(Object[]::new));
            }
        }
    }
}
