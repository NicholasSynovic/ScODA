build:
	git describe --tags --abbrev=0 | tail -n 1 | xargs -I % uv version %
	rm -rf dist/
	uv build
	uv pip install dist/*.tar.gz

create-dev:
	pre-commit install
	pre-commit autoupdate
	git submodule update --init --recursive
	rm -rf .venv
	uv sync
	uv build

download-jdbc:
	wget -O src/jdbc/postgres.jar https://jdbc.postgresql.org/download/postgresql-42.7.7.jar --no-clobber; true
	wget -O src/jdbc/mysql_archive.tar.gz https://cdn.mysql.com//Downloads/Connector-J/mysql-connector-j-9.3.0.tar.gz --no-clobber; true

	(\
		cd src/jdbc; \
		tar -xvf mysql_archive.tar.gz mysql-connector-j-9.3.0/mysql-connector-j-9.3.0.jar; \
		mv mysql-connector-j-9.3.0/mysql-connector-j-9.3.0.jar mysql.jar; \
		rm -rf mysql-connector-j-9.3.0; \
	)
