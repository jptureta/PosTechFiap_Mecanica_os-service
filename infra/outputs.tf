output "database_endpoint" {
  value = aws_db_instance.service.endpoint
}

output "database_security_group_id" {
  value = aws_security_group.service_db.id
}

output "rabbitmq_url" {
  value     = aws_mq_broker.service_messaging.instances[0].endpoints[0]
  sensitive = true
}

output "diagnostics_table_name" {
  value = aws_dynamodb_table.diagnostics.name
}
