# reformatted and added layer - Arif Syed 15/8/24

resource "aws_s3_bucket" "ingested_data_bucket" {
  bucket = var.ingested_data_bucket
  tags = {
    Name = "ingestion-bucket"
  }
}

resource "aws_s3_bucket" "onyx_processed_bucket" {
  bucket = var.processed_data_bucket
  tags = {
    Name = "processed-bucket"
  }
}

resource "aws_s3_bucket" "onyx_lambda_code_bucket" {
  bucket = var.lambda_code_bucket
  tags = {
    Name = "lambda-code-bucket"
  }
}

# # upload zip file to s3
# resource "aws_s3_object" "layer_code" {
#   bucket     = aws_s3_bucket.onyx_lambda_code_bucket.bucket
#   key        = "${var.extract_lambda}/extract_layer.zip"
#   source     = "${path.module}/../src/extract_lambda/extract_layer.zip"
#   depends_on = [null_resource.lambda_layer] # triggered only if the zip file is created
# }

# resource "aws_s3_bucket_notification" "aws-transform-trigger" {
#   bucket = aws_s3_bucket.ingested_data_bucket.id
#   lambda_function {
#     lambda_function_arn = aws_lambda_function.transform_lambda.arn
#     events              = ["s3:ObjectCreated:*"]
#   }
# }