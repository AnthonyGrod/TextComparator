## Overview

Locality sensitive hashing does not work well with short strings.

Because questions have very similar form, there are very high chances that
questions' bands will be hashed to the same bucket.

Out of curiosity, I tried my LSH algorithm on the first 1000 questions (sorted by
ids) and it performed far better in terms of precision compared to running
on whole dataset (recall and precision was aroung 40%). 
My guess is that first 1000 questions are superficially more dense in terms 
of actual duplicate questions, compared to the whole dataset.

## Results table

| Shingle size | Signature size | Number of bands | TP     | FP      |
|--------------|----------------|-----------------|--------|---------|
| 5            | 100            | 20              | 31533  | 2006090 |
| 8            | 1000           | 100             | 12143  | 520169  |


## Running the program
Docker configuration is provided in `Dockerfile` and `docker-compose.yml`.
In order to run it, one needs to execute:
```adlanguage
docker build -t lsh-app:latest .
docker run -p 8080:4040 --rm lsh-app:latest
```