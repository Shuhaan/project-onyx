variable "ingested_data_bucket" {
  type    = string
  default = "onyx-totesys-ingested-data-bucket"
}

variable "lambda_code_bucket" {
  type    = string
  default = "onyx-lambda-code-bucket"
}

variable "extract_lambda" {
  type    = string
  default = "extract"
}

variable "extract_lambda_role_arn" {
  type = string
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}
