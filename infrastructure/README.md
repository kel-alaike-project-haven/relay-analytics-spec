# 🏗️ Relay Infrastructure Repository

## Overview
This repository contains **Infrastructure as Code (IaC)** for provisioning and managing all resources as part of the demos ingestion and analytics platform.

It uses **Terraform** with a **modular structure** to provision Google Cloud resources, enabling:
- **Reusable, consistent deployments** across environments
- **Version-controlled infrastructure** changes
- **Automated service provisioning** for development, staging, and production

---

## 📂 Repository Structure

```
├── environments
│   └── development
│       ├── backend.tf             # Remote state configuration
│       ├── locals.tf              # Local variables & constants
│       ├── main.tf                # Root Terraform config for environment
│       ├── terraform.tfvars       # Environment-specific variables
│       └── variables.tf           # Input variable definitions
│
├── modules
│   ├── cloud_build                # Manages GCP Cloud Build pipelines
│   │   ├── config/cloud_build.json
│   │   ├── main.tf
│   │   ├── providers.tf
│   │   └── variables.tf
│   │
│   ├── cloud_run                  # Manages Cloud Run service deployments
│   │   ├── config/cloud_run.json
│   │   ├── main.tf
│   │   ├── providers.tf
│   │   └── variables.tf
│   │
│   ├── cloud_storage              # Manages GCS buckets
│   │   ├── config/cloud_storage.json
│   │   ├── main.tf
│   │   ├── providers.tf
│   │   └── variables.tf
│   │
│   ├── datasets                   # Creates BigQuery datasets
│   │   ├── config/datasets.json
│   │   ├── main.tf
│   │   ├── providers.tf
│   │   └── variables.tf
│   │
│   └── pubsub                     # Configures Pub/Sub topics & subscriptions
│       ├── config/pubsub_topics.json
│       ├── config/pubsub_subscriptions.json
│       ├── main.tf
│       ├── providers.tf
│       └── variables.tf
│
└── scripts
    └── create-service-accounts.sh  # Helper script to create GCP service accounts
```

---

## 🚀 Getting Started

### 1. Prerequisites
- [Terraform](https://developer.hashicorp.com/terraform/downloads) **v1.3+**
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
- Access to a GCP Project with:
  - Billing enabled
  - Owner/Editor permissions for provisioning resources

### 2. Change Directory to the correct location
```bash
cd relay-infrastructure/environments/development
```

### 3. Authenticate with GCP
```bash
gcloud auth application-default login
gcloud config set project <PROJECT_ID>
```

### 4. Initialise Terraform
```bash
terraform init
```

### 5. Plan & Apply Changes
```bash
terraform plan
terraform apply
```

---

## 🛠 Module Details

### **Cloud Build**
- Provisions **Cloud Build triggers** for continuous deployment
- Uses JSON config files in `modules/cloud_build/config` to define trigger parameters
- Automatically rebuilds and deploys services on changes to `main` branch

### **Cloud Run**
- Deploys containerised services from **Artifact Registry**
- Configurable memory, CPU, concurrency, and ingress settings

### **Cloud Storage**
- Creates GCS buckets for cold storage and archival
- Applies lifecycle policies for retention and storage class transitions

### **Datasets**
- Creates BigQuery datasets with correct location and access control
- JSON config allows easy addition/removal of datasets without changing Terraform code

### **Pub/Sub**
- Creates topics and subscriptions (push or pull)
- Supports subscription ordering and dead-letter queues (DLQs)

---

## ⚡ Deployment Flow with Cloud Build & Artifact Registry
1. Developer pushes code to **GitHub** (or VCS)
2. Cloud Build trigger runs, building the Docker image
3. Image is pushed to **Artifact Registry**
4. Terraform/Cloud Build updates the **Cloud Run** service with the latest image

---

## 🧪 Testing Infrastructure Changes
We recommend testing changes in the **development** environment before promoting them to staging or production.

```bash
cd environments/development
terraform plan
terraform apply
```

---

## 📌 Notes
- All JSON configs are **data-driven**, so adding new resources is as simple as updating the config and re-running Terraform.
- **No direct console changes** — all infrastructure changes should be made via Terraform for auditability.
