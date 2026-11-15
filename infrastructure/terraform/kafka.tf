resource "aws_security_group" "kafka" {
  count       = var.enable_msk ? 1 : 0
  name        = "${local.name}-kafka"
  description = "MSK Serverless, from job tasks only"
  vpc_id      = aws_vpc.main.id
  ingress {
    description     = "Kafka IAM listener from jobs"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.jobs.id]
  }
}

resource "aws_msk_serverless_cluster" "main" {
  count        = var.enable_msk ? 1 : 0
  cluster_name = local.name
  vpc_config {
    subnet_ids         = aws_subnet.private[*].id
    security_group_ids = [aws_security_group.kafka[0].id]
  }
  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }
}
