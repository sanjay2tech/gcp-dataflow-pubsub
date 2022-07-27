package com.bachinalabs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.apache.beam.sdk.values.PCollection;

import com.google.auth.oauth2.ServiceAccountCredentials;

public class StreamingPipepline {

    public static void main( String[] args ) throws FileNotFoundException, IOException {

        // Start by defining the options for the pipeline.
        DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

        // For Cloud execution, set the Cloud Platform project, staging location,
        // and specify DataflowRunner.
        options.setProject("gcpdemo-355300");
        options.setRegion("us-central1");
        options.setStagingLocation("gs://sm_dataflow_temp/binaries/");
        options.setGcpTempLocation("gs://sm_dataflow_temp/temp/");
        options.setNetwork("default");
        options.setSubnetwork("regions/us-central1/subnetworks/default");
        options.setRunner(DataflowRunner.class);

        // Then create the pipeline.
        Pipeline p = Pipeline.create(options);

        p.apply("Read PubSub Messages", PubsubIO.readMessagesWithAttributesAndMessageId().fromTopic("projects/gcpdemo-355300/topics/TOPIC_DATAFLOW_FILES"))
        		 .apply(new ReadPubSubMessages());
                
        p.apply("Read xlsx file from bucket", FileIO.match().filepattern("gs://sm_dataflow_input/*.xlsx"))
            .apply(FileIO.readMatches())
            .apply(new ReadXlsx())
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))))
            .apply("Write Bucket Names", TextIO.write().to("gs://sm_dataflow_output/")
                   .withWindowedWrites().withNumShards(1));

        p.run().waitUntilFinish();

    }
}
