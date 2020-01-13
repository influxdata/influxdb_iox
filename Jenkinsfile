pipeline {
    agent {
        node {
            label 'rust-build'
        }
    }
    stages {
        stage('Build') {
            steps {
              container('rust') {
                sh "cargo build"  
              }
            }
        }
        stage('Clippy') {
            steps {
                container('rust') {
                    sh "cargo +nightly clippy --all"   
                }
            }
        }
        stage('Rustfmt') {
            steps {
                container('rust') {
                    // The build will fail if rustfmt thinks any changes are
                    // required.
                    sh "cargo +nightly fmt --all -- --write-mode diff"   
                }
            }
        }
        stage('Test') {
            steps {
                container('rust') {
                    sh "cargo test"
                }
            }
        }
    }
}