output "lakehouse_bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "catalog_endpoint" {
  value = aws_db_instance.catalog.address
}

output "ecr_repositories" {
  value = { for k, r in aws_ecr_repository.images : k => r.repository_url }
}

output "ecs_cluster" {
  value = aws_ecs_cluster.main.arn
}

output "spark_task_definition" {
  value = aws_ecs_task_definition.spark_job.arn
}
