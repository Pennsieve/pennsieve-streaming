#!groovy

node("executor") {
    checkout scm
    def authorName = sh(returnStdout: true, script: 'git --no-pager show --format="%an" --no-patch')
    def commitHash  = sh(returnStdout: true, script: 'git rev-parse HEAD | cut -c-7').trim()
    def imageTag = "${env.BUILD_NUMBER}-${commitHash}"

    def sbt = "sbt -Dsbt.log.noformat=true"
    def pennsieveNexusCreds = usernamePassword(
        credentialsId: "pennsieve-nexus-ci-login",
        usernameVariable: "PENNSIEVE_NEXUS_USER",
        passwordVariable: "PENNSIEVE_NEXUS_PW"
    )

    try {
        stage("Build") {
            withCredentials([pennsieveNexusCreds]) {
                sh "$sbt clean compile"
            }
        }

        stage("Test") {
            withCredentials([pennsieveNexusCreds]) {
                sh "$sbt test"
                junit 'target/test-reports/*.xml'
            }
        }

        stage("Docker") {
            withCredentials([pennsieveNexusCreds]) {
                sh "$sbt clean docker"
            }

            sh "docker tag pennsieve/pennsieve-streaming:latest pennsieve/pennsieve-streaming:$imageTag"

            sh "docker push pennsieve/pennsieve-streaming:latest"
            sh "docker push pennsieve/pennsieve-streaming:$imageTag"
        }

        if (["master", "dev"].contains(env.BRANCH_NAME)) {
            stage("Deploy") {
                build job: "service-deploy/pennsieve-non-prod/us-east-1/dev-vpc-use1/dev/pennsieve-streaming",
                parameters: [
                    string(name: 'IMAGE_TAG', value: imageTag),
                    string(name: 'TERRAFORM_ACTION', value: 'apply')
                ]
            }
        }
    } catch (e) {
        slackSend(color: '#b20000', message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL}) by ${authorName}")
        throw e
    }
    slackSend(color: '#006600', message: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL}) by ${authorName}")
}
