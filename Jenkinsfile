pipeline {
    agent any
    stages {
        stage('Checkout') {
            script {
                steps {
                    git branch: 'master'
                    url: 'https://github.com/ajvan2808/MapReduce.git'
                }
            }
        }
    }
}