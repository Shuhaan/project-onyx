variable "ingested_data_bucket" {
  type    = string
  default = "onyx-totesys-ingested-data-bucket"
}

variable "ingested_data_bucket_arn" {
  type = string
}

variable "processed_data_bucket" {
  type    = string
  default = "onyx-processed-data-bucket"
}

variable "lambda_code_bucket" {
  type    = string
  default = "onyx-lambda-code-bucket"
}

variable "transform_lambda" {
  type    = string
  default = "transform"
}

variable "transform_lambda_role_arn" {
  type = string
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}
