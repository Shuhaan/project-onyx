provider "aws" {
  region = "eu-west-2" 
}


terraform {
  backend "s3" {
    bucket         = "project-onyx-tfstate"  
    key            = "terraform.tfstate"  
    region         = "eu-west-2"  
    encrypt        = true

  }
}