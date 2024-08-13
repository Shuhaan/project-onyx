resource "aws_s3_bucket" "onyx_ingestion_bucket" {
  bucket = "onyx_ingestion_bucket"
  tags = {
    Name = "ingestion-bucket"
  }
}

resource "aws_s3_bucket" "onyx_processed_bucket" {
  bucket = "onyx_processed_bucket"
  tags = {
    Name = "processed-bucket"
  }
}

resource "aws_s3_bucket" "onyx_lambda_code_bucket" {
  bucket = "onyx_lambda_code_bucket"
  tags = {
    Name = "lambda-code-bucket"
  }
}

# resource "aws_s3_bucket_notification" "bucket_notification" {
#   bucket = aws_s3_bucket.data_bucket.id

#   lambda_function {
#     lambda_function_arn = aws_lambda_function.quote_handler.arn
#     events              = ["s3:ObjectCreated:*"]
#   }

#   depends_on = [aws_lambda_permission.allow_s3]
# }