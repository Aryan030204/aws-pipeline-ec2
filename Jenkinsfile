pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
        timeout(time: 30, unit: 'MINUTES')
    }

    environment {
        COMPOSE_PROJECT_NAME = 'aws-pipeline-ec2'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Test') {
            agent {
                docker {
                    image 'python:3.11-slim'
                    args '-u root'
                    reuseNode true
                }
            }
            steps {
                sh '''
                    apt-get update && apt-get install -y gcc default-libmysqlclient-dev build-essential pkg-config
                    pip install --no-cache-dir -r requirements.txt pytest
                    python -m pytest tests -v --junitxml=test-results.xml
                '''
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: 'test-results.xml'
                }
            }
        }

        // Only reached when every test above passed.
        stage('Deploy') {
            when {
                branch 'master'
            }
            steps {
                // .env is gitignored; supply it as a Jenkins "Secret file" credential.
                withCredentials([file(credentialsId: 'aws-pipeline-env-file', variable: 'ENV_FILE')]) {
                    sh '''
                        cp "$ENV_FILE" .env
                        docker network inspect pipeline-net >/dev/null 2>&1 || docker network create pipeline-net

                        # Stop the old containers, then start the newest code.
                        docker compose down
                        docker compose up -d --build
                        docker compose ps
                    '''
                }
            }
            post {
                always {
                    sh 'rm -f .env'
                }
            }
        }
    }

    post {
        failure {
            echo 'Build failed - tests or deploy did not pass; old containers are untouched unless the Deploy stage had started.'
        }
    }
}
