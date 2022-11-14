#!/bin/bash

psql -U datawaves -c "CREATE TABLE flowers (flowersId serial PRIMARY KEY, sepal_length decimal,sepal_width decimal,petal_length decimal,petal_width decimal,species VARCHAR(50));"
psql -U datawaves -c "\copy flowers (sepal_length, sepal_width, petal_length, petal_width, species) FROM '/data/flowers.csv' CSV HEADER;"