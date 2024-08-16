# reformatted and modified file - Arif Syed 15/8/24

#modified archive to include all py files excluding transform.py
# data "archive_file" "extract_lambda" {
#   type                = "zip"
#   output_file_mode    = "0666"
#   source_dir          = "${path.module}/../src"
#   output_path         = "${path.module}/../extract.zip"
#   excludes            = ["${path.module}/../transform.py"]
# }

locals {
  source_files        = ["${path.module}/../src/connection.py", "${path.module}/../src/utils.py", "${path.module}/../src/extract.py"]
}

data "template_file" "t_file" {
  count               = "${length(local.source_files)}"
  template            = "${file(element(local.source_files, count.index))}"
}


data "archive_file" "extract_lambda" {
  type                = "zip"
  output_file_mode    = "0666"
  output_path         = "${path.module}/../extract.zip"

  source {
    filename          = "${basename(local.source_files[0])}"
    content           = "${data.template_file.t_file.0.rendered}"
  }

  source {
    filename          = "${basename(local.source_files[1])}"
    content           = "${data.template_file.t_file.1.rendered}"
  }

  source {
    filename          = "${basename(local.source_files[2])}"
    content           = "${data.template_file.t_file.2.rendered}"
  }

}

# increased timeout to 60 seconds and added layer plus environment
resource "aws_lambda_function" "extract_handler" {
  filename            = "${path.module}/../extract.zip"
  function_name       = "extract"
  role                = aws_iam_role.extract_lambda_role.arn
  handler             = "extract.lambda_handler"
  source_code_hash    = data.archive_file.extract_lambda.output_base64sha256
  runtime             = var.python_runtime
  timeout             = 60
  layers              = [aws_lambda_layer_version.extract_layer.arn]
  environment {
    variables = {
       S3_BUCKET_NAME = aws_s3_bucket.ingested_data_bucket.bucket
     }
   }
}

# # added layer setup
# data "archive_file" "layer" {
#   type                = "zip"
#   output_file_mode    = "0666"
#   source_dir          = "${path.module}/../layer"
#   output_path         = "${path.module}/../layer.zip"
# }

#define variables
locals {
  layer_path        = "../layer"
  layer_zip_name    = "../layer.zip"
  layer_name        = "extract_layer"
  requirements_name = "requirements.in"
  requirements_path = "${path.module}/../${local.requirements_name}"
}

# create zip file from requirements.txt. Triggers only when the file is updated
resource "null_resource" "lambda_layer" {
  triggers = {
    requirements = filesha1(local.requirements_path)
  }

  # the command to install python and dependencies to the machine and zips
  provisioner "local-exec" {
    command = <<EOT
      cd ../layer
      rm -rf python
      mkdir python
      pip install -r ../requirements.in -t python/
      zip -r ../layer.zip python/
    EOT
  }
}

# create lambda layer from s3 object
resource "aws_lambda_layer_version" "extract_layer" {
  layer_name          = "extract_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.onyx_lambda_code_bucket.bucket
  s3_key              = aws_s3_object.layer_code.key
  depends_on          = [aws_s3_object.layer_code] # triggered only if the zip file is uploaded to the bucket
}
