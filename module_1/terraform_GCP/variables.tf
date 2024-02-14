variable "credentials" {
    description = "My credentials"
    default = "./.GC_creds/my-creds.json"
}

variable "project_name" {
    description = "Name of the project"
    default = "fresh-ocean-412204"
}

variable "project_region" {
  description = "region of the project"
  default = "us-centrall"
}

variable "location" {
    description = "Project location"
    default = "US"
}

variable "bq_dataset_name" {
    description = "My BigQuery dataset name"
    default = "demo_dataset"
}

variable "project" {
  description = "Name of the project"
  type        = string
}