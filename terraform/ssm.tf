// TIMESERIES DATA PIPELINE CONFIG

resource "aws_ssm_parameter" "cloudfront_url" {
  name  = "/${var.environment_name}/${var.service_name}/cloudfront-url"
  type  = "String"
  value = "https://${data.terraform_remote_state.platform_infrastructure.outputs.private_timeseries_route53_record}/"
}

resource "aws_ssm_parameter" "cloudfront_host" {
  name  = "/${var.environment_name}/${var.service_name}/cloudfront-host"
  type  = "String"
  value = data.terraform_remote_state.platform_infrastructure.outputs.private_timeseries_route53_record
}

resource "aws_ssm_parameter" "parallelism" {
  name  = "/${var.environment_name}/${var.service_name}/parallelism"
  type  = "String"
  value = "8"
}

resource "aws_ssm_parameter" "timeseries_throttle_items" {
  name  = "/${var.environment_name}/${var.service_name}/timeseries-throttle-items"
  type  = "String"
  value = "100"
}

resource "aws_ssm_parameter" "timeseries_throttle_period_seconds" {
  name  = "/${var.environment_name}/${var.service_name}/timeseries-throttle-period-seconds"
  type  = "String"
  value = "10"
}

resource "aws_ssm_parameter" "send_spike_threshold" {
  name  = "/${var.environment_name}/${var.service_name}/send-spike-threshold"
  type  = "String"
  value = "10"
}

resource "aws_ssm_parameter" "query_limit" {
  name  = "/${var.environment_name}/${var.service_name}/query-limit"
  type  = "String"
  value = "100000"
}

# WEBSERVER CONFIG

resource "aws_ssm_parameter" "timeseries_port" {
  name  = "/${var.environment_name}/${var.service_name}/timeseries-port"
  type  = "String"
  value = "8080"
}

resource "aws_ssm_parameter" "timeseries_address" {
  name  = "/${var.environment_name}/${var.service_name}/timeseries-address"
  type  = "String"
  value = "0.0.0.0"
}

resource "aws_ssm_parameter" "idle_timeout" {
  name  = "/${var.environment_name}/${var.service_name}/idle-timeout"
  type  = "String"
  value = "3600s"
}

resource "aws_ssm_parameter" "default_gap_threshold" {
  name  = "/${var.environment_name}/${var.service_name}/default-gap-threshold"
  type  = "String"
  value = "2.0"
}

resource "aws_ssm_parameter" "request_queue_size" {
  name  = "/${var.environment_name}/${var.service_name}/request-queue-size"
  type  = "String"
  value = "100"
}

# AKKA

resource "aws_ssm_parameter" "akka_log_level" {
  name  = "/${var.environment_name}/${var.service_name}/akka-log-level"
  type  = "String"
  value = "INFO"
}

resource "aws_ssm_parameter" "akka_stream_debug_logging" {
  name  = "/${var.environment_name}/${var.service_name}/akka-stream-debug-logging"
  type  = "String"
  value = "off"
}

# POSTGRES

resource "aws_ssm_parameter" "data_postgres_database" {
  name  = "/${var.environment_name}/${var.service_name}/data-postgres-database"
  type  = "String"
  value = var.data_postgres_database
}

resource "aws_ssm_parameter" "data_postgres_host" {
  name  = "/${var.environment_name}/${var.service_name}/data-postgres-host"
  type  = "String"
  value = data.terraform_remote_state.pennsieve_postgres.outputs.master_fqdn
}

resource "aws_ssm_parameter" "data_postgres_port" {
  name  = "/${var.environment_name}/${var.service_name}/data-postgres-port"
  type  = "String"
  value = data.terraform_remote_state.pennsieve_postgres.outputs.master_port
}

resource "aws_ssm_parameter" "data_postgres_user" {
  name  = "/${var.environment_name}/${var.service_name}/data-postgres-user"
  type  = "String"
  value = "${var.environment_name}_${replace(var.service_name, "-", "_")}_user"
}

resource "aws_ssm_parameter" "data_postgres_password" {
  name  = "/${var.environment_name}/${var.service_name}/data-postgres-password"
  type  = "SecureString"
  value = "dummy"

  lifecycle {
    ignore_changes = [value]
  }
}

resource "aws_ssm_parameter" "postgres_database" {
  name  = "/${var.environment_name}/${var.service_name}/postgres-database"
  type  = "String"
  value = var.pennsieve_postgres_database
}

resource "aws_ssm_parameter" "postgres_host" {
  name  = "/${var.environment_name}/${var.service_name}/postgres-host"
  type  = "String"
  value = data.terraform_remote_state.pennsieve_postgres.outputs.master_fqdn
}

resource "aws_ssm_parameter" "postgres_port" {
  name  = "/${var.environment_name}/${var.service_name}/postgres-port"
  type  = "String"
  value = data.terraform_remote_state.pennsieve_postgres.outputs.master_port
}

resource "aws_ssm_parameter" "postgres_user" {
  name  = "/${var.environment_name}/${var.service_name}/postgres-user"
  type  = "String"
  value = "${var.environment_name}_${replace(var.service_name, "-", "_")}_user"
}

resource "aws_ssm_parameter" "postgres_password" {
  name  = "/${var.environment_name}/${var.service_name}/postgres-password"
  type  = "SecureString"
  value = "dummy"

  lifecycle {
    ignore_changes = [value]
  }
}

// JWT KEY
resource "aws_ssm_parameter" "jwt_key" {
  name  = "/${var.environment_name}/${var.service_name}/jwt-key"
  type  = "SecureString"
  value = "im just a pretend jwt key I actually got set from the aws console"

  lifecycle {
    ignore_changes = [value]
  }
}

// DISCOVER API CONFIGURATION

resource "aws_ssm_parameter" "discover_api_host" {
  name = "/${var.environment_name}/${var.service_name}/discover-api-host"
  type = "String"

  value = "https://${data.terraform_remote_state.discover_api.outputs.internal_fqdn}"
}
