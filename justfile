[working-directory: '.']
verify input="data/otto-tiny.jsonl":
    @sbt assembly
    @bash -o pipefail -c 'spark-submit --class project.Skeleton --master "local[*]" target/scala-2.13/myProject.jar "$1" | python3 scripts/check_output.py' -- {{quote(input)}}
