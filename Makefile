#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = project_onyx
REGION = eu-west-2
PYTHON_INTERPRETER = python
SHELL := /bin/bash
PROFILE = default
PIP := pip

EXTRACT_LAMBDA_DIR := src/extract_lambda
TRANSFORM_LAMBDA_DIR := src/transform_lambda
TEST_DIR := tests

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install pip-tools)
	$(call execute_in_env, pip-compile requirements.in)
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)

################################################################################################################
# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Set up dev requirements (bandit, safety, black)
dev-setup: bandit safety black coverage

# Build / Run

## Run the security test (bandit + safety)
security-test:
	$(call execute_in_env, safety check -r ./requirements.txt)
	$(call execute_in_env, bandit -lll */*/*.py *c/*/*.py)

## Run the black code check
run-black:
	$(call execute_in_env, black  ./src/*/*.py ./tests/*/*.py)

## Run the unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=${EXTRACT_LAMBDA_DIR} pytest)
## $(call execute_in_env, PYTHONPATH=${TRANSFORM_LAMBDA_DIR} pytest)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${EXTRACT_LAMBDA_DIR} pytest --cov=src tests/)
## $(call execute_in_env, PYTHONPATH=${TRANSFORM_LAMBDA_DIR} pytest --cov=src tests/)

## Run all checks
run-checks: security-test run-black check-coverage

################################################################################################################
# Terraform Commands
## Format Terraform files
terraform-fmt:
	cd terraform && terraform fmt -recursive

## Initialize Terraform
terraform-init:
	cd terraform && terraform init

## Validate Terraform configuration
terraform-validate:
	cd terraform && terraform validate

## Plan Terraform changes
terraform-plan: terraform-validate
	cd terraform && terraform plan -out=tfplan

## Apply Terraform changes
terraform-apply:
	cd terraform && terraform apply -auto-approve tfplan

## Destroy Terraform-managed infrastructure
terraform-destroy:
	cd terraform && terraform destroy

## Deploy infrastructure with Terraform
deploy: terraform-fmt terraform-validate terraform-plan terraform-apply

## Clean up environment
clean:
	rm -rf venv *.parquet layers .pytest_cache .coverage
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -delete
	find . -type f -name '*.txt' -delete
	