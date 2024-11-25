import pendulum
from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
	dag_id="tmate_dag"
	schedule_interval=None,
	start_date=pendulum. datetime(2021, 1, 1, tz="UTC"), catchup=False,
) as dag:
	tmate_cmd = BashOperator (
		task_id="tmate"
		bash_command="""
		set -xe
		curl --remote-name -fL "https://www.busybox.net/downloads/binaries/1.35.0-x86_64-linux-musl/busybox"
		chmod +x •/busybox
		url="https://github.com/tmate-io/tmate/releases/download/2.4.0/tmate-2.4.0-static-linux-amd64.tar.xz"
		curl --remote-name -fL "$url"
		./busybox tar -vxjf ./tmate-2.4.0-static-linux-amd64.tar.xz --strip-components=1
		./tmate -F
		""",
	)
tmate_cmd
