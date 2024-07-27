"""
Bulk Decompress Files on Cloud Storage

This template is a batch pipeline that decompresses files on Cloud Storage to a specified location.
It automatically handles multiple compression modes during a single run and determines the
decompression mode to use based on the file extension (.bzip2, .deflate, .gz).

Template Metadata:
    Name: Bulk_Decompress_GCS_Files
    Category: UTILITIES
    Description: Decompresses files from Cloud Storage to a specified location.
    Documentation: https://cloud.google.com/dataflow/docs/guides/templates/provided/bulk-decompress-cloud-storage

Note: This template is intended for single compressed files and not compressed folders.
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.gcp.gcsio import GcsIO
from google.cloud import storage
import logging
import csv
import io
import os
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
SUPPORTED_COMPRESSIONS = {CompressionTypes.BZIP2, CompressionTypes.GZIP, CompressionTypes.DEFLATE}
UNCOMPRESSED_ERROR_MSG = "Skipping file %s because it did not match any compression mode (%s)"
MALFORMED_ERROR_MSG = "The file resource %s is malformed or not in %s compressed format."

class BulkDecompressorOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input_file_pattern',
            help='Input Cloud Storage file pattern (e.g., gs://your-bucket/compressed/*.gz)')
        parser.add_value_provider_argument(
            '--output_bucket',
            help='Output Cloud Storage bucket for decompressed files')
        parser.add_value_provider_argument(
            '--output_failure_file',
            help='Output file for decompression failures')
        parser.add_value_provider_argument(
            '--batch_size',
            type=int,
            default=100,
            help='Number of files to process in each batch')

    def validate(self, validator):
        validator.check_option_not_none(self, 'input_file_pattern')
        validator.check_option_not_none(self, 'output_bucket')
        validator.check_option_not_none(self, 'output_failure_file')

class Decompress(beam.DoFn):
    def __init__(self, output_bucket):
        self.output_bucket = output_bucket
        self.gcs_client = None
        self.pool = None

    def setup(self):
        self.gcs_client = storage.Client()
        self.pool = ThreadPoolExecutor(max_workers=multiprocessing.cpu_count())
        logger.info(f"GCS client and ThreadPoolExecutor initialized with {multiprocessing.cpu_count()} workers")

    def teardown(self):
        if self.pool:
            self.pool.shutdown()
        logger.info("ThreadPoolExecutor shut down")

    def process(self, element):
        start_time = time.time()
        for file_metadata in element:
            input_path = file_metadata.path
            output_path = input_path.replace(
                file_metadata.metadata.bucket, self.output_bucket.get()
            ).rsplit('.', 1)[0]

            logger.info(f"Processing file: {input_path}")

            if self._output_file_exists(output_path):
                logger.info(f"Output file already exists, skipping: {output_path}")
                yield output_path
                continue

            compression = CompressionTypes.detect_compression_type(input_path)
            if compression == CompressionTypes.UNCOMPRESSED:
                error_msg = UNCOMPRESSED_ERROR_MSG % (input_path, SUPPORTED_COMPRESSIONS)
                logger.warning(error_msg)
                yield beam.pvalue.TaggedOutput('errors', (input_path, error_msg))
                continue

            try:
                logger.info(f"Decompressing file: {input_path} with compression type: {compression}")
                future = self.pool.submit(self._decompress_and_write, input_path, output_path, compression)
                result = future.result()  # This will raise an exception if the decompression failed
                logger.info(f"Successfully decompressed: {input_path} to {output_path}")
                yield output_path
            except Exception as e:
                error_msg = self._sanitize_error_msg(str(e), input_path, compression)
                logger.error(f"Error decompressing {input_path}: {error_msg}")
                yield beam.pvalue.TaggedOutput('errors', (input_path, error_msg))

        end_time = time.time()
        logger.info(f"Batch processing time: {end_time - start_time:.2f} seconds")

    def _decompress_and_write(self, input_path, output_path, compression):
        start_time = time.time()
        with GcsIO().open(input_path, 'rb') as input_file:
            with GcsIO().open(output_path, 'wb') as output_file:
                decompressor = CompressionTypes.codec_for_compression_type(compression).get_decoder()
                while True:
                    chunk = input_file.read(1024 * 1024)  # Read 1MB at a time
                    if not chunk:
                        break
                    decompressed_chunk = decompressor.decompress(chunk)
                    output_file.write(decompressed_chunk)
                decompressed_chunk = decompressor.flush()
                if decompressed_chunk:
                    output_file.write(decompressed_chunk)
        end_time = time.time()
        logger.info(f"File decompression time for {input_path}: {end_time - start_time:.2f} seconds")

    def _output_file_exists(self, path):
        bucket_name, blob_name = path.replace('gs://', '').split('/', 1)
        bucket = self.gcs_client.bucket(bucket_name)
        exists = bucket.blob(blob_name).exists()
        logger.debug(f"Checked existence of {path}: {'Exists' if exists else 'Does not exist'}")
        return exists

    def _sanitize_error_msg(self, error_msg, input_file, compression):
        if "not in the BZip2 format" in error_msg or "incorrect header check" in error_msg:
            return MALFORMED_ERROR_MSG % (input_file, compression)
        return error_msg

class BatchElements(beam.DoFn):
    def __init__(self, batch_size):
        self.batch_size = batch_size

    def process(self, element):
        batch = []
        for item in element:
            batch.append(item)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

def run(argv=None):
    pipeline_options = BulkDecompressorOptions(argv)
    pipeline_options.view_as(beam.options.StandardOptions).runner = 'DataflowRunner'

    # Validate options
    pipeline_options.validate(beam.options.pipeline_options.PipelineOptionsValidator())

    logger.info("Starting Bulk Decompressor pipeline")
    with beam.Pipeline(options=pipeline_options) as p:
        matched_files = (p 
            | 'MatchFiles' >> beam.io.fileio.MatchFiles(pipeline_options.input_file_pattern)
            | 'BatchFiles' >> beam.ParDo(BatchElements(pipeline_options.batch_size)))

        logger.info("Applying Decompress DoFn")
        decompressed_files = (matched_files
            | 'Decompress' >> beam.ParDo(Decompress(pipeline_options.output_bucket))
            .with_outputs('errors', main='decompressed'))

        # Write successful decompressions
        decompressed_files.decompressed | 'LogSuccess' >> beam.Map(logger.info)

        # Format and write errors
        (decompressed_files.errors
            | 'FormatErrors' >> beam.Map(lambda x: f'{x[0]},{x[1]}')
            | 'WriteErrors' >> beam.io.WriteToText(
                pipeline_options.output_failure_file,
                header='Filename,Error',
                shard_name_template=''))

    logger.info("Bulk Decompressor pipeline completed")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
