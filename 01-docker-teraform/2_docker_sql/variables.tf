variable "credentials" {
  description = "My credentials"
  default     = "../../keys/my_cred.json"

}

variable "project" {
  description = "Project ID"
  default     = "project-de-pnt"

}

variable "region" {
  description = "Region run service"
  default     = "ASIA-SOUTHEAST1"
}

variable "location" {
  description = "Project Location"
  default     = "ASIA-SOUTHEAST1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default = "project-de-pnt-data-lake-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}