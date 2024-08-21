# reformatted and modified file - Arif Syed 15/8/24

locals {
  source_files = [
    "${path.module}/../../src/transform_lambda/transform_utils.py",
  "${path.module}/../../src/transform_lambda/transform.py"]
}

data "template_file" "t_file" {
  count    = length(local.source_files)
  template = file(element(local.source_files, count.index))
}


data "archive_file" "transform_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  output_path      = "${path.module}/../../src/transform_lambda/transform.zip"

  source {
    filename = basename(local.source_files[0])
    content  = data.template_file.t_file.0.rendered
  }

  source {
    filename = basename(local.source_files[1])
    content  = data.template_file.t_file.1.rendered
  }
}

# increased timeout to 60 seconds and added layer plus environment
resource "aws_lambda_function" "transform_handler" {
  filename         = "${path.module}/../../src/transform_lambda/transform.zip"
  function_name    = "transform"
  role             = var.transform_lambda_role_arn
  handler          = "transform.lambda_handler"
  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  runtime          = var.python_runtime
  timeout          = 60
  layers           = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:13"]
  environment {
    variables = {
      S3_BUCKET_NAME = var.processed_data_bucket
    }
  }
}

# #define variables
# locals {
#   layer_path        = "transform_layer"
#   layer_zip_name    = "transform_layer.zip"
#   layer_name        = "transform_layer"
#   requirements_name = "requirements.lambda"
#   requirements_path = "${path.module}/../../src/transform_lambda/${local.requirements_name}"
# }

# # create zip file from requirements.txt. Triggers only when the file is updated
# resource "null_resource" "lambda_layer" {
#   triggers = {
#     requirements = filesha1(local.requirements_path)
#   }

#   # the command to install python and dependencies to the machine and zips
#   provisioner "local-exec" {
#     command     = <<EOF
#       rm -rf layer
#       mkdir layer
#       cd layer
#       mkdir python
#       pip install -r ../requirements.lambda -t python/
#       zip -r ../transform_layer.zip python/
#     EOF
#     working_dir = "${path.module}/../../src/transform_lambda"
#   }
# }

# # create lambda layer from s3 object
# resource "aws_lambda_layer_version" "transform_layer" {
#   layer_name          = "transform_layer"
#   compatible_runtimes = [var.python_runtime]
#   s3_bucket           = var.lambda_code_bucket
#   s3_key              = aws_s3_object.layer_code.key
#   depends_on          = [aws_s3_object.layer_code] # triggered only if the zip file is uploaded to the bucket
# }
