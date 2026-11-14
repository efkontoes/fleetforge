# One PostgreSQL instance holds the Iceberg JDBC catalog and Airflow's metadata.
resource "aws_db_subnet_group" "main" {
  name       = local.name
  subnet_ids = aws_subnet.private[*].id
}

resource "aws_security_group" "db" {
  name        = "${local.name}-db"
  description = "Catalog PostgreSQL, from job tasks only"
  vpc_id      = aws_vpc.main.id
  ingress {
    description     = "PostgreSQL from jobs"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.jobs.id]
  }
}

resource "aws_db_parameter_group" "main" {
  name   = local.name
  family = "postgres17"
  parameter {
    name  = "rds.force_ssl"
    value = "1"
  }
  parameter {
    name  = "log_min_duration_statement"
    value = "1000"
  }
}

resource "aws_db_instance" "catalog" {
  identifier                          = "${local.name}-catalog"
  engine                              = "postgres"
  engine_version                      = "17"
  instance_class                      = var.db_instance_class
  allocated_storage                   = 20
  max_allocated_storage               = 100
  storage_type                        = "gp3"
  storage_encrypted                   = true
  kms_key_id                          = aws_kms_key.lake.arn
  db_name                             = "iceberg_catalog"
  username                            = "iceberg"
  manage_master_user_password         = true
  master_user_secret_kms_key_id       = aws_kms_key.lake.arn
  iam_database_authentication_enabled = true
  db_subnet_group_name                = aws_db_subnet_group.main.name
  vpc_security_group_ids              = [aws_security_group.db.id]
  parameter_group_name                = aws_db_parameter_group.main.name
  multi_az                            = var.db_multi_az
  backup_retention_period             = 7
  copy_tags_to_snapshot               = true
  deletion_protection                 = true
  skip_final_snapshot                 = false
  final_snapshot_identifier           = "${local.name}-catalog-final"
  performance_insights_enabled        = true
  performance_insights_kms_key_id     = aws_kms_key.lake.arn
  auto_minor_version_upgrade          = true
  enabled_cloudwatch_logs_exports     = ["postgresql"]
}
