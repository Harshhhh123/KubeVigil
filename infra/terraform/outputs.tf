output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "rds_endpoint" {
  value = aws_db_instance.kubevigil.endpoint
}

output "ecr_watcher" {
  value = aws_ecr_repository.watcher.repository_url
}

output "ecr_drift_engine" {
  value = aws_ecr_repository.drift_engine.repository_url
}

output "ecr_ai_agent" {
  value = aws_ecr_repository.ai_agent.repository_url
}

output "ecr_dashboard_backend" {
  value = aws_ecr_repository.dashboard_backend.repository_url
}

output "argocd_namespace" {
  value = "argocd"
}