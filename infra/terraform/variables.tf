variable "region" {
  default = "ap-south-1"
}

variable "cluster_name" {
  default = "kubevigil"
}

variable "db_password" {
  default   = "kubevigil123"
  sensitive = true
}

variable "github_owner" {
  default = "Harshhhh123"
}

variable "github_repo" {
  default = "KubeVigil"
}