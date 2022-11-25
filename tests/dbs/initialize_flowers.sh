#!/bin/bash

psql -U datawaves -c "CREATE TABLE flowers (flowersId serial PRIMARY KEY, sepal_length real,sepal_width real,petal_length real,petal_width real,species VARCHAR(50));"
psql -U datawaves -c "\copy flowers (sepal_length, sepal_width, petal_length, petal_width, species) FROM '/data/flowers.csv' CSV HEADER;"