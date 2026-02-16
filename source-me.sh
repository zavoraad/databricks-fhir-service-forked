export JAVA_HOME=/opt/homebrew/Cellar/openjdk@17/17.0.15/libexec/openjdk.jdk/Contents/Home
export PATH=/opt/homebrew/Cellar/openjdk@17/17.0.15/bin:${PATH}
#export token=
#export jdbc=
## First build the Docker image manually:
#docker build -t databricks-fhir-api:latest .

## Then run the integration tests:
#sbt testDocker
