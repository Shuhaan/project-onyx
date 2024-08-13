variable "ingested_data_bucket" {
  type    = string
  default = "onyx_totesys_ingested_data_bucket"
}

variable "processed_data_bucket" {
  type    = string
  default = "onyx_processed_data_bucket"
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
