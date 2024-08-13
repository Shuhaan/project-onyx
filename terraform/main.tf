terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "project-onyx-tfstate"
    key = "terraform/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName = "Data Warehouse from Totesys"
      Team = "Project Onyx"
      DeployedFrom = "Terraform"
      Repository = "https://github.com/Shuhaan/project-onyx"
      CostCentre = "DE"
      Environment = "dev"
    }
  }
}
