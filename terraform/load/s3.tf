# upload zip file to s3
resource "aws_s3_object" "layer_code" {
  bucket     = var.lambda_code_bucket
  key        = "${var.load_lambda}/load_layer.zip"
  source     = "${path.module}/../../src/load_lambda/load_layer.zip"
  depends_on = [null_resource.lambda_layer] # triggered only if the zip file is created
}

