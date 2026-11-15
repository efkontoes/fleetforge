terraform {
  required_version = ">= 1.16.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.69.1"
    }
  }
}

provider "aws" {
  region = var.region

  # offline_plan lets CI run a full plan with dummy credentials: nothing below reads AWS.
  skip_credentials_validation = var.offline_plan
  skip_requesting_account_id  = var.offline_plan
  skip_metadata_api_check     = var.offline_plan

  default_tags {
    tags = {
      project     = "fleetforge"
      environment = var.environment
      managed_by  = "terraform"
    }
  }
}
