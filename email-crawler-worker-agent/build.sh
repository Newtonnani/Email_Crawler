aws ecr get-login-password --region us-west-2 | sudo docker login --username AWS --password-stdin 368860953605.dkr.ecr.us-west-2.amazonaws.com
sudo docker build -t docker-email-wrkr-agent .
sudo docker tag docker-email-wrkr-agent:latest 368860953605.dkr.ecr.us-west-2.amazonaws.com/docker-email-wrkr-agent:latest
sudo docker push 368860953605.dkr.ecr.us-west-2.amazonaws.com/docker-email-wrkr-agent:latest
