locals {
  extract_lambda_source_dir = "${path.module}/../src/extract_lambda"
  extract_zip_path          = "${path.module}/../extract.zip"
  layer_zip_path            = "${path.module}/../layers/extract_layer.zip"

  # List of source files for the Lambda function
  source_files = [
    "${local.extract_lambda_source_dir}/connection.py",
    "${local.extract_lambda_source_dir}/utils.py",
    "${local.extract_lambda_source_dir}/extract.py"
  ]
}

data "archive_file" "extract_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  output_path      = local.extract_zip_path

  # Use a dynamic block to iterate over the source_files list
  dynamic "source" {
    for_each = local.source_files
    content {
      content  = file(source.value)
      filename = basename(source.value)
    }
  }
}

# Create a Lambda function with the created ZIP file
resource "aws_lambda_function" "extract_handler" {
  filename         = local.extract_zip_path
  function_name    = "extract"
  role             = aws_iam_role.extract_lambda_role.arn
  handler          = "extract.lambda_handler"
  source_code_hash = data.archive_file.extract_lambda.output_base64sha256
  runtime          = var.python_runtime
  timeout          = 60
  layers           = [aws_lambda_layer_version.extract_layer.arn]
  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.ingested_data_bucket.bucket
    }
  }
}

# Create the Lambda Layer
resource "aws_lambda_layer_version" "extract_layer" {
  layer_name          = "extract_layer"
  compatible_runtimes = [var.python_runtime]
  filename            = local.layer_zip_path
  source_code_hash    = filebase64sha256(local.layer_zip_path)
}