# Install ArgoCD via Helm
resource "helm_release" "argocd" {
  name             = "argocd"
  repository       = "https://argoproj.github.io/argo-helm"
  chart            = "argo-cd"
  namespace        = "argocd"
  create_namespace = true
  version          = "5.51.0"

  depends_on = [module.eks]
}

# ArgoCD Application — watches k8s/eks/ folder in GitHub
resource "kubernetes_manifest" "kubevigil_app" {
  manifest = {
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "kubevigil"
      namespace = "argocd"
    }
    spec = {
      project = "default"
      source = {
        repoURL        = "https://github.com/${var.github_owner}/${var.github_repo}"
        targetRevision = "HEAD"
        path           = "k8s/eks"
      }
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "kubevigil"
      }
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = ["CreateNamespace=true"]
      }
    }
  }

  depends_on = [helm_release.argocd]
}