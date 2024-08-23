# reformatted and modified file - Arif Syed 15/8/24

locals {
  source_files = ["${path.module}/../../src/load_lambda/load_utils.py",
  "${path.module}/../../src/load_lambda/load.py"]
}

data "template_file" "t_file_load" {
  count    = length(local.source_files)
  template = file(element(local.source_files, count.index))
}


data "archive_file" "load_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  output_path      = "${path.module}/../../src/load_lambda/load.zip"

  source {
    filename = basename(local.source_files[0])
    content  = data.template_file.t_file_load.0.rendered
  }

  source {
    filename = basename(local.source_files[1])
    content  = data.template_file.t_file_load.1.rendered
  }
}

# increased timeout to 60 seconds and added layer plus environment
resource "aws_lambda_function" "load_handler" {
  filename         = "${path.module}/../../src/load_lambda/load.zip"
  function_name    = "load"
  role             = var.load_lambda_role_arn
  handler          = "load.lambda_handler"
  source_code_hash = data.archive_file.load_lambda.output_base64sha256
  runtime          = var.python_runtime
  timeout          = 300
  layers           = [aws_lambda_layer_version.load_layer.arn, "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:13"]
  environment {
    variables = {
      S3_BUCKET_NAME = var.processed_data_bucket
    }
  }
}

#define variables
locals {
  layer_path        = "load_layer"
  layer_zip_name    = "load_layer.zip"
  layer_name        = "load_layer"
  requirements_name = "requirements.lambda"
  requirements_path = "${path.module}/../../src/load_lambda/${local.requirements_name}"
}

# create zip file from requirements.txt. Triggers only when the file is updated
resource "null_resource" "lambda_layer" {
  triggers = {
    requirements = filesha1(local.requirements_path)
  }

  # the command to install python and dependencies to the machine and zips
  provisioner "local-exec" {
    command     = <<EOF
      rm -rf layer
      mkdir layer
      cd layer
      mkdir python
      pip install -r ../requirements.lambda -t python/
      zip -r ../load_layer.zip python/
    EOF
    working_dir = "${path.module}/../../src/load_lambda"
  }
}

# create lambda layer from s3 object
resource "aws_lambda_layer_version" "load_layer" {
  layer_name          = "load_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = var.lambda_code_bucket
  s3_key              = aws_s3_object.layer_code.key
  depends_on          = [aws_s3_object.layer_code] # triggered only if the zip file is uploaded to the bucket
}
