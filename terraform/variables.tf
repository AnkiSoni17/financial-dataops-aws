variable "aws_region" {
  description = "AWS region where resources will be created"
  type        = string
  default     = "eu-west-2"
}

variable "project_name" {
  description = "Name used to identify project resources"
  type        = string
  default     = "financial-dataops"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

