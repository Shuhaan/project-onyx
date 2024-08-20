terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "project-onyx-tfstate"
    key    = "terraform/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName  = "Data Warehouse from Totesys"
      Team         = "Project Onyx"
      TeamMembers  = "Hasan-Arif-Ayub-Saif-Ewan-Shuhaan"
      DeployedFrom = "Terraform"
      Repository   = "https://github.com/Shuhaan/project-onyx"
      CostCentre   = "DE"
      Environment  = "dev"
    }
  }
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}

module "extract" {
  source                  = "./extract"
  extract_lambda_role_arn = aws_iam_role.extract_lambda_role.arn
}

module "transform" {
  source                    = "./transform"
  ingested_data_bucket_arn  = aws_s3_bucket.ingested_data_bucket.arn
  transform_lambda_role_arn = aws_iam_role.transform_lambda_role.arn
}