.PHONY: test
test:
	@echo "Running tests..."
	python -m pytest

.PHONY: install
install:
	@echo "Install requirement... It recommended to do the installation inside virtual environment"
	python -m pip install -r requirements.txt
	@echo "Checking for Java installation..."
	@if java -version > /dev/null 2>&1; then \
		echo "Java is installed."; \
	else \
		echo "Java is not installed. Please follow this guide to install Java: https://docs.oracle.com/javase/8/docs/technotes/guides/install/install_overview.html"; \
	fi
