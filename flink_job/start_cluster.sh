FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager"

docker network create flink-network

docker run -d --rm \
    --name=jobmanager \
    --network flink-network \
    --publish 8081:8081 \
    -v /home/theodorecurtil/acosom_assessment/flink_job:/opt/flink/jobs \
    --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
    theodorecurtil/pyflink:1.16-py3.7.9 jobmanager

docker run \
    --rm \
    --name=taskmanager \
    --network flink-network \
    -v /home/theodorecurtil/acosom_assessment/flink_job:/opt/flink/jobs \
    --env FLINK_PROPERTIES="${FLINK_PROPERTIES}" \
    theodorecurtil/pyflink:1.16-py3.7.9 taskmanager
