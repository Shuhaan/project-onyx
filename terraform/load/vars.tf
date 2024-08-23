
variable "processed_data_bucket" {
  type    = string
  default = "onyx-processed-data-bucket"
}


variable "processed_data_bucket_arn" {
  type = string
}
variable "lambda_code_bucket" {
  type    = string
  default = "onyx-lambda-code-bucket"
}

variable "load_lambda" {
  type    = string
  default = "load"
}

variable "load_lambda_role_arn" {
  type = string
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}
