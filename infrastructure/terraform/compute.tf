resource "aws_ecr_repository" "images" {
  for_each             = toset(["spark", "dbt", "airflow", "ml"])
  name                 = "${local.name}/${each.key}"
  image_tag_mutability = "IMMUTABLE"
  image_scanning_configuration {
    scan_on_push = true
  }
  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = aws_kms_key.lake.arn
  }
}

resource "aws_ecs_cluster" "main" {
  name = local.name
  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "jobs" {
  name              = "/${local.name}/jobs"
  retention_in_days = var.log_retention_days
  kms_key_id        = aws_kms_key.lake.arn
}

data "aws_iam_policy_document" "ecs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "execution" {
  name               = "${local.name}-task-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy_attachment" "execution" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "execution_secrets" {
  role = aws_iam_role.execution.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = "secretsmanager:GetSecretValue", Resource = aws_db_instance.catalog.master_user_secret[0].secret_arn },
      { Effect = "Allow", Action = "kms:Decrypt", Resource = aws_kms_key.lake.arn },
    ]
  })
}

# What the jobs themselves may do: read and write the lakehouse prefixes, nothing else.
resource "aws_iam_role" "jobs" {
  name               = "${local.name}-jobs"
  assume_role_policy = data.aws_iam_policy_document.ecs_assume.json
}

resource "aws_iam_role_policy" "jobs" {
  role = aws_iam_role.jobs.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:ListBucket"], Resource = aws_s3_bucket.lake.arn },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
        Resource = ["${aws_s3_bucket.lake.arn}/warehouse/*", "${aws_s3_bucket.lake.arn}/exports/*"]
      },
      { Effect = "Allow", Action = ["kms:Decrypt", "kms:GenerateDataKey"], Resource = aws_kms_key.lake.arn },
    ]
  })
}

resource "aws_ecs_task_definition" "spark_job" {
  family                   = "${local.name}-spark-job"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.spark_task_cpu
  memory                   = var.spark_task_memory
  execution_role_arn       = aws_iam_role.execution.arn
  task_role_arn            = aws_iam_role.jobs.arn
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }
  container_definitions = jsonencode([{
    name       = "job"
    image      = "${aws_ecr_repository.images["spark"].repository_url}:${var.environment}"
    essential  = true
    entryPoint = ["python", "-m"]
    command    = ["fleetforge.lakehouse.jobs.silver_build"]
    environment = [
      { name = "FLEETFORGE_LAKEHOUSE_CATALOG_JDBC_URL", value = "jdbc:postgresql://${aws_db_instance.catalog.address}:5432/iceberg_catalog?sslmode=require" },
      { name = "FLEETFORGE_LAKEHOUSE_WAREHOUSE", value = "s3://${aws_s3_bucket.lake.bucket}/warehouse" },
      { name = "FLEETFORGE_LAKEHOUSE_S3_ENDPOINT", value = "https://s3.${var.region}.amazonaws.com" },
      { name = "FLEETFORGE_LAKEHOUSE_S3_REGION", value = var.region },
      { name = "MALLOC_ARENA_MAX", value = "2" },
    ]
    secrets = [
      { name = "FLEETFORGE_LAKEHOUSE_CATALOG_PASSWORD", valueFrom = "${aws_db_instance.catalog.master_user_secret[0].secret_arn}:password::" },
    ]
    readonlyRootFilesystem = false
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.jobs.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "spark"
      }
    }
  }])
}
