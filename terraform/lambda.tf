data "archive_file" "extract_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../src/extract.py"
  output_path      = "${path.module}/../extract.zip"
}

resource "aws_lambda_function" "extract_handler" {
  filename      = "${path.module}/../extract.zip"
  function_name = "extract"
  role          = aws_iam_role.extract_lambda_role.arn
  handler       = "extract.lambda_handler"

  source_code_hash = data.archive_file.lambda.output_base64sha256

  runtime = var.python_runtime

  timeout = 10

  # environment {
  #   variables = {
  #     S3_BUCKET_NAME = "${aws_s3_bucket.data_bucket.bucket}"
  #   }
  # }
}
