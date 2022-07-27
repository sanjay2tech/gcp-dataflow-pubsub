package com.bachinalabs;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.FileIO;

public class ReadXlsx extends PTransform<PCollection<FileIO.ReadableFile>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<FileIO.ReadableFile> input) {

        return input.apply(ParDo.of(new ReadXlsxDoFn()));
    }
}
