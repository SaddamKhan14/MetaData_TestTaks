build:
    mkdir ./target
    cp ./jobs/elt_*.py ./target
    cd ./ && zip -x jobs/elt_*.py -r ../target/jobs.zip .