# upload zip file to s3
resource "aws_s3_object" "layer_code" {
  bucket     = var.lambda_code_bucket
  key        = "${var.extract_lambda}/extract_layer.zip"
  source     = "${path.module}/../../src/extract_lambda/extract_layer.zip"
  depends_on = [null_resource.lambda_layer] # triggered only if the zip file is created
}

