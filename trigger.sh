#!/bin.bash
echo "process intiated"
if [ ! -d "./dags" ] || [ ! -d "./test" ]; then
	echo "'dags' File does not exists."
	
fi
echo "File exists."

pytest --cov=. . --cov-report=html
echo "process done"
# Copy coverage.xml to sonar path.