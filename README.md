# Enhanced Bulk GCS File Decompressor

## Overview

This project is an enhanced version of Google's Bulk Decompress Cloud Storage Files Dataflow template. It builds upon the original template, adding new features while maintaining the core functionality provided by Google.

## Key Enhancements

- **Flexible Output Structure**: Unlike the original template which outputs to a single directory, this version preserves the original directory structure of the input files.
- **Resumability**: Implements a check for existing decompressed files, allowing jobs to be resumed without redundant processing.
- **Efficient Processing**: Skips already decompressed files, saving time and resources on subsequent runs.

## Original Features (Maintained from Google's Template)

- Decompresses files from Google Cloud Storage (GCS)
- Supports multiple compression formats: BZIP2, DEFLATE, GZIP
- Provides error logging for failed decompression attempts

## Prerequisites

- Google Cloud Platform account
- Google Cloud SDK installed and configured
- Java Development Kit (JDK) 11 or later
- Apache Maven

## Setup

1. Clone this repository:
   ```
   git clone https://github.com/your-username/enhanced-bulk-gcs-decompressor.git
   cd enhanced-bulk-gcs-decompressor
   ```

2. Set up your Google Cloud project:
   ```
   export PROJECT_ID=your-project-id
   export REGION=your-preferred-region
   gcloud config set project $PROJECT_ID
   ```

3. Create a staging bucket (if not already existing):
   ```
   export STAGING_BUCKET=gs://$PROJECT_ID-dataflow-staging
   gsutil mb -p $PROJECT_ID -l $REGION $STAGING_BUCKET
   ```

## Building the Template

Build the enhanced Dataflow template using Maven:

```
mvn clean package -DskipTests -Dexec.mainClass=com.google.cloud.teleport.templates.BulkDecompressor -Dexec.args="--runner=DataflowRunner --project=$PROJECT_ID --stagingLocation=$STAGING_BUCKET/staging --templateLocation=$STAGING_BUCKET/templates/BulkDecompressor --region=$REGION"
```

## Running the Job

Execute the Dataflow job using the following command:

```
gcloud dataflow jobs run bulk-decompress-job \
  --gcs-location=$STAGING_BUCKET/templates/BulkDecompressor \
  --region=$REGION \
  --parameters \
inputFilePattern=gs://your-input-bucket/path/to/files/*/*/*/*.gz,\
outputBucket=gs://your-output-bucket,\
outputFailureFile=gs://your-output-bucket/failures.csv
```

Replace `your-input-bucket` and `your-output-bucket` with your actual GCS bucket names. Note that the input file pattern can now include multiple directory levels.

## Monitoring

Monitor your Dataflow job using:

```
gcloud dataflow jobs list --region=$REGION
```

Or visit the Google Cloud Console for a visual representation of the job's progress.

## Output

Decompressed files will be written to the specified output bucket, maintaining the original directory structure. For example, an input file at `gs://input-bucket/2023/05/01/data.json.gz` will be decompressed to `gs://output-bucket/2023/05/01/data.json`. Any decompression failures will be logged in the specified failure file.

## Contributing

While contributions to improve the template are welcome, please be mindful that this is an enhanced version of Google's original work. Significant changes should be considered carefully to maintain compatibility and the spirit of the original template.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details. The original template is the work of Google LLC and is subject to their licensing terms.

## Acknowledgments

This project is based on the Bulk Decompress Cloud Storage Files template created by Google. We express our gratitude to the original authors for their valuable work, which served as the foundation for these enhancements.

For the original template and other Google-provided Dataflow templates, please visit:
[Google Cloud Dataflow Templates](https://github.com/GoogleCloudPlatform/DataflowTemplates)
