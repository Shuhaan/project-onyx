# reformatted and added layer - Arif Syed 15/8/24

resource "aws_s3_bucket" "ingested_data_bucket" {
  bucket        = var.ingested_data_bucket
  force_destroy = true
  tags = {
    Name = "ingestion-bucket"
  }

}

resource "aws_s3_bucket" "processed_data_bucket" {
  bucket        = var.processed_data_bucket
  force_destroy = true
  tags = {
    Name = "processed-bucket"
  }
}

resource "aws_s3_bucket" "onyx_lambda_code_bucket" {
  bucket        = var.lambda_code_bucket
  force_destroy = true
  tags = {
    Name = "lambda-code-bucket"
  }
}
