// Import necessary libraries and packages
package com.google.cloud.teleport.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.templates.BulkDecompressor.Options;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Define the template metadata
@Template(
    name = "Bulk_Decompress_GCS_Files",
    category = TemplateCategory.UTILITIES,
    displayName = "Bulk Decompress Files on Cloud Storage",
    description = {
      "The Bulk Decompress Cloud Storage Files template is a batch pipeline that decompresses files on Cloud Storage to a specified location. "
          + "This functionality is useful when you want to use compressed data to minimize network bandwidth costs during a migration, but would like to maximize analytical processing speed by operating on uncompressed data after migration. "
          + "The pipeline automatically handles multiple compression modes during a single run and determines the decompression mode to use based on the file extension (.bzip2, .deflate, .gz, .zip).",
      "Note: The Bulk Decompress Cloud Storage Files template is intended for single compressed files and not compressed folders."
    },
    optionsClass = Options.class,
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/bulk-decompress-cloud-storage",
    contactInformation = "https://cloud.google.com/support",
    requirements = {
      "The files to decompress must be in one of the following formats: Bzip2, Deflate, and Gzip.",
      "The output bucket must exist prior to running the pipeline."
    })
public class BulkDecompressor {

  // Logger for this class
  private static final Logger LOG = LoggerFactory.getLogger(BulkDecompressor.class);

  // Define supported compression types
  @VisibleForTesting
  static final Set<Compression> SUPPORTED_COMPRESSIONS =
      Stream.of(Compression.values())
          .filter(value -> value != Compression.AUTO && value != Compression.UNCOMPRESSED)
          .collect(Collectors.toSet());

  // Error message for uncompressed files
  @VisibleForTesting
  static final String UNCOMPRESSED_ERROR_MSG =
      "Skipping file %s because it did not match any compression mode (%s)";

  // Error message for malformed compressed files
  @VisibleForTesting
  static final String MALFORMED_ERROR_MSG =
      "The file resource %s is malformed or not in %s compressed format.";

  // Define output tags for the main decompression output and errors
  @VisibleForTesting
  static final TupleTag<String> DECOMPRESS_MAIN_OUT_TAG = new TupleTag<String>() {};

  @VisibleForTesting
  static final TupleTag<KV<String, String>> DEADLETTER_TAG = new TupleTag<KV<String, String>>() {};

  // Define pipeline options interface
  public interface Options extends PipelineOptions {
    @TemplateParameter.GcsReadFile(
        order = 1,
        groupName = "Source",
        description = "Input Cloud Storage File(s)",
        helpText = "The Cloud Storage location of the files you'd like to process.",
        example = "gs://your-bucket/your-files/*.gz")
    @Required
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @TemplateParameter.GcsWriteFolder(
        order = 2,
        groupName = "Target",
        description = "Output bucket in Cloud Storage",
        helpText = "The Cloud Storage bucket where decompressed files will be written, maintaining the original directory structure.",
        example = "gs://your-output-bucket")
    @Required
    ValueProvider<String> getOutputBucket();

    void setOutputBucket(ValueProvider<String> value);

    @TemplateParameter.GcsWriteFile(
        order = 3,
        description = "The output file for failures during the decompression process",
        helpText =
            "The output file to write failures to during the decompression process. If there are no failures, the file will still be created but will be empty. The contents will be one line for each file which failed decompression in CSV format (Filename, Error). Note that this parameter will allow the pipeline to continue processing in the event of a failure.",
        example = "gs://your-bucket/decompressed/failed.csv")
    @Required
    ValueProvider<String> getOutputFailureFile();

    void setOutputFailureFile(ValueProvider<String> value);
  }

  // Main method to run the pipeline
  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  // Method to set up and run the pipeline
  public static PipelineResult run(Options options) {
    Pipeline pipeline = Pipeline.create(options);

    // Define the main decompression logic
    PCollectionTuple decompressOut =
        pipeline
            .apply("MatchFile(s)", FileIO.match().filepattern(options.getInputFilePattern()))
            .apply(
                "DecompressFile(s)",
                ParDo.of(new Decompress(options.getOutputBucket()))
                    .withOutputTags(DECOMPRESS_MAIN_OUT_TAG, TupleTagList.of(DEADLETTER_TAG)));

    // Handle and format errors
    decompressOut
        .get(DEADLETTER_TAG)
        .apply(
            "FormatErrors",
            MapElements.into(TypeDescriptors.strings())
                .via(
                    kv -> {
                      StringWriter stringWriter = new StringWriter();
                      try {
                        CSVPrinter printer =
                            new CSVPrinter(
                                stringWriter,
                                CSVFormat.DEFAULT
                                    .withEscape('\\')
                                    .withQuoteMode(QuoteMode.NONE)
                                    .withRecordSeparator('\n'));
                        printer.printRecord(kv.getKey(), kv.getValue());
                      } catch (IOException e) {
                        throw new RuntimeException(e);
                      }

                      return stringWriter.toString();
                    }))
        .apply(
            "WriteErrorFile",
            TextIO.write()
                .to(options.getOutputFailureFile())
                .withHeader("Filename,Error")
                .withoutSharding());

    return pipeline.run();
  }

  // DoFn class for decompression logic
  @SuppressWarnings("serial")
  public static class Decompress extends DoFn<MatchResult.Metadata, String> {
    private final ValueProvider<String> outputBucket;

    // Constructor
    Decompress(ValueProvider<String> outputBucket) {
      this.outputBucket = outputBucket;
    }

    // Main processing method
    @ProcessElement
    public void processElement(ProcessContext context) {
      ResourceId inputFile = context.element().resourceId();
      ResourceId outputFile = getOutputFile(inputFile);

      // Check if output file already exists
      if (outputFileExists(outputFile)) {
        context.output(outputFile.toString());
        return;
      }

      // Check if input file is compressed
      if (!Compression.AUTO.isCompressed(inputFile.toString())) {
        String errorMsg = String.format(UNCOMPRESSED_ERROR_MSG, inputFile.toString(), SUPPORTED_COMPRESSIONS);
        context.output(DEADLETTER_TAG, KV.of(inputFile.toString(), errorMsg));
        return;
      }

      // Attempt decompression
      try {
        decompress(inputFile, outputFile);
        context.output(outputFile.toString());
      } catch (IOException e) {
        LOG.error(e.getMessage());
        context.output(DEADLETTER_TAG, KV.of(inputFile.toString(), e.getMessage()));
      }
    }

    // Helper method to get the output file path
    private ResourceId getOutputFile(ResourceId inputFile) {
      String inputPath = inputFile.toString();
      String outputPath = inputPath.replace(inputFile.getBucket(), outputBucket.get());
      return FileSystems.matchNewResource(Files.getNameWithoutExtension(outputPath), false);
    }

    // Helper method to check if output file exists
    private boolean outputFileExists(ResourceId outputFile) {
      try {
        FileSystems.match(outputFile.toString());
        return true;
      } catch (IOException e) {
        return false;
      }
    }

    // Helper method to perform decompression
    private void decompress(ResourceId inputFile, ResourceId outputFile) throws IOException {
      Compression compression = Compression.detect(inputFile.toString());
      
      try (ReadableByteChannel readerChannel = compression.readDecompressed(FileSystems.open(inputFile))) {
        try (WritableByteChannel writerChannel = FileSystems.create(outputFile, MimeTypes.TEXT)) {
          ByteStreams.copy(readerChannel, writerChannel);
        }
      } catch (IOException e) {
        String msg = e.getMessage();
        LOG.error("Error occurred during decompression of {}", inputFile.toString(), e);
        throw new IOException(sanitizeDecompressionErrorMsg(msg, inputFile, compression));
      }
    }

    // Helper method to sanitize decompression error messages
    private String sanitizeDecompressionErrorMsg(
        @Nullable String errorMsg, ResourceId inputFile, Compression compression) {
      if (errorMsg != null
          && (errorMsg.contains("not in the BZip2 format")
              || errorMsg.contains("incorrect header check"))) {
        errorMsg = String.format(MALFORMED_ERROR_MSG, inputFile.toString(), compression);
      }
      return errorMsg == null ? "" : errorMsg;
    }
  }
}
