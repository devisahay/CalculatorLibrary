#!/bin.bash
echo "process intiated"
if [ ! -d "./dags" ] || [ ! -d "./test" ]; then
	echo "'dags' File does not exists."
	
fi
echo "File exists."
echo "File list are.."
ls -la /home/airflow/gcs/dags
echo "here are some test files..."
ls -la /home/airflow/gcs/dags/test*
echo "listing done... starting pytest..."
pytest --cov=/home/airflow/gcs/dags/. /home/airflow/gcs/dags/test* --cov-report=html:/tmp/htmlcov  --cov-report=xml:/tmp/coverage.xml
echo "process done"
ls -la /tmp/
# Copy coverage.xml to sonar path.