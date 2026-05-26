module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.0.0"

  name = "kubevigil-vpc"
  cidr = "192.168.0.0/16"

  azs             = ["ap-south-1a", "ap-south-1b"]
  private_subnets = ["192.168.1.0/24", "192.168.2.0/24"]
  public_subnets  = ["192.168.3.0/24", "192.168.4.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true

  tags = {
    Project = "kubevigil"
  }
}