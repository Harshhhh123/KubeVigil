resource "aws_ecr_repository" "watcher" {
  name         = "kubevigil/watcher"
  force_delete = true
  tags         = { Project = "kubevigil" }
}

resource "aws_ecr_repository" "drift_engine" {
  name         = "kubevigil/drift-engine"
  force_delete = true
  tags         = { Project = "kubevigil" }
}

resource "aws_ecr_repository" "ai_agent" {
  name         = "kubevigil/ai-agent"
  force_delete = true
  tags         = { Project = "kubevigil" }
}

resource "aws_ecr_repository" "dashboard_backend" {
  name         = "kubevigil/dashboard-backend"
  force_delete = true
  tags         = { Project = "kubevigil" }
}