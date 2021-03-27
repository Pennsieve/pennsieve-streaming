variable "aws_account" {}

variable "environment_name" {}

variable "service_name" {}

variable "vpc_name" {}

variable "pennsieve_postgres_database" {
  default = "pennsieve_postgres"
}

variable "data_postgres_database" {
  default = "data_postgres"
}

variable "ecs_task_iam_role_id" {}

variable "external_alb_listener_rule_priority" {
  default = 6
}

