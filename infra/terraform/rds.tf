# Security group for RDS
resource "aws_security_group" "rds" {
  name   = "kubevigil-rds-sg"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [module.eks.cluster_security_group_id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Project = "kubevigil" }
}

# RDS subnet group
resource "aws_db_subnet_group" "kubevigil" {
  name       = "kubevigil-subnet-group"
  subnet_ids = module.vpc.private_subnets
  tags       = { Project = "kubevigil" }
}

# RDS instance
resource "aws_db_instance" "kubevigil" {
  identifier        = "kubevigil-db"
  engine            = "postgres"
  engine_version    = "15"
  instance_class    = "db.t3.micro"
  allocated_storage = 20

  db_name  = "kubevigil"
  username = "postgres"
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.kubevigil.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  skip_final_snapshot = true
  publicly_accessible = false

  tags = { Project = "kubevigil" }
}