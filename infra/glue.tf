
resource "aws_cloudwatch_log_group" "glue_log_group" {
  name              = "/aws-glue/jobs/output"
  retention_in_days = 14
  
  tags = {
    Environment = "ESGI"
    Project     = "ESGI-Spark"
  }
}

resource "aws_glue_job" "spark_job" {
  name     = "spark_job_pf"
  role_arn = aws_iam_role.glue.arn

  command {
    script_location = "s3://${aws_s3_bucket.bucket.id}/spark-jobs/exo2_glue_job.py"
  }

  glue_version = "3.0"
  number_of_workers = 2
  worker_type = "Standard"
  
  default_arguments = {
    "--additional-python-modules"       = "s3://${aws_s3_bucket.bucket.id}/wheel/spark-handson.whl"
    "--python-modules-installer-option" = "--upgrade"
    "--job-language"                    = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                 = "true"
    "--input_path"                      = "s3://${aws_s3_bucket.bucket.id}/data/sell.csv"
    "--output_bucket"                   = aws_s3_bucket.bucket.id
    "--continuous-log-logGroup"         = "/aws-glue/jobs/output"
  }

  execution_property {
    max_concurrent_runs = 1
  }
}