



aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <accountn_aws_id>.dkr.ecr.us-east-1.amazonaws.com

docker build -t pipeline-polars-img-generator . # here

docker tag pipeline-polars-img-generator:latest <accountn_aws_id>.dkr.ecr.us-east-1.amazonaws.com/pipeline-polars-rep-generator:latest

docker push <accountn_aws_id>.dkr.ecr.us-east-1.amazonaws.com/pipeline-polars-rep-generator:latest