resource "aws_s3_bucket" "bucket" {
  bucket = "esgi-spark-pf-bucket"
  
  tags = {
    Name        = "ESGI Spark Bucket"
    Environment = "ESGI"
  }
}