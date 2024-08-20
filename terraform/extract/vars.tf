variable "ingested_data_bucket" {
  type    = string
  default = "onyx-totesys-ingested-data-bucket"
}

variable "processed_data_bucket" {
  type    = string
  default = "onyx-processed-data-bucket"
}

variable "lambda_code_bucket" {
  type    = string
  default = "onyx-lambda-code-bucket"
}

variable "extract_lambda" {
  type    = string
  default = "extract"
}

variable "transform_lambda" {
  type    = string
  default = "transform"
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}
