variable "region" {
  type    = string
  default = "eu-west-1"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "account_id" {
  description = "AWS account id (a variable, so the configuration never has to look it up)."
  type        = string
}

variable "availability_zones" {
  type    = list(string)
  default = ["eu-west-1a", "eu-west-1b"]
}

variable "vpc_cidr" {
  type    = string
  default = "10.40.0.0/16"
}

variable "db_instance_class" {
  type    = string
  default = "db.t4g.small"
}

variable "db_multi_az" {
  type    = bool
  default = false
}

variable "enable_msk" {
  description = "Create an MSK Serverless cluster (billed per cluster-hour while it exists)."
  type        = bool
  default     = false
}

variable "spark_task_cpu" {
  type    = number
  default = 1024
}

variable "spark_task_memory" {
  type    = number
  default = 3072
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "offline_plan" {
  description = "Skip every provider call that needs real credentials (CI plans only)."
  type        = bool
  default     = false
}
