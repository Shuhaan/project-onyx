# upload zip file to s3
resource "aws_s3_object" "layer_code" {
  bucket     = var.lambda_code_bucket
  key        = "${var.extract_lambda}/extract_layer.zip"
  source     = "${path.module}/../src/extract_lambda/extract_layer.zip"
  depends_on = [null_resource.lambda_layer] # triggered only if the zip file is created
}

resource "aws_s3_bucket_notification" "aws-transform-trigger" {
  bucket = var.ingested_data_bucket
  lambda_function {
    lambda_function_arn = aws_lambda_function.transform_lambda.arn
    events              = ["s3:ObjectCreated:*"]
  }
}