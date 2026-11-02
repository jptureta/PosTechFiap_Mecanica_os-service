output "database_endpoint" {
  value = aws_db_instance.service.endpoint
}

output "database_security_group_id" {
  value = aws_security_group.service_db.id
}

output "os_events_queue_url" {
  value = aws_sqs_queue.os_events.url
}

output "os_events_dlq_url" {
  value = aws_sqs_queue.os_events_dlq.url
}

output "diagnostics_table_name" {
  value = aws_dynamodb_table.diagnostics.name
}
