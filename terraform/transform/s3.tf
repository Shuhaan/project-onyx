# # upload zip file to s3
# resource "aws_s3_object" "layer_code" {
#   bucket     = var.lambda_code_bucket
#   key        = "${var.transform_lambda}/transform_layer.zip"
#   source     = "${path.module}/../../src/transform_lambda/transform_layer.zip"
#   depends_on = [null_resource.lambda_layer] # triggered only if the zip file is created
# }

resource "aws_s3_bucket_notification" "aws-transform-trigger" {
  bucket = var.ingested_data_bucket
  lambda_function {
    lambda_function_arn = aws_lambda_function.transform_handler.arn
    events              = ["s3:ObjectCreated:*"]
  }
  depends_on = [aws_lambda_permission.allow_ingested_data_bucket]
}

resource "aws_lambda_permission" "allow_ingested_data_bucket" {
  statement_id  = "AllowExecutionFromS3IngestedDataBucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transform_handler.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = var.ingested_data_bucket_arn
}