# GitHub Actions Workflows

This directory contains GitHub Actions workflows for automated testing, linting, and validation of the dabs project.

## Databricks Unit Tests Workflow

The `databricks-test.yml` workflow runs unit tests from the `tests/` folder directly on Databricks clusters for every pull request. This ensures that your code works correctly in the actual Databricks runtime environment.

### Features
- Runs pytest on Databricks clusters
- Generates coverage reports
- Posts test results as PR comments
- Uses OAuth for secure authentication

### Requirements

Configure these secrets in your GitHub repository:
- `DATABRICKS_INSTANCE`: Your Databricks workspace hostname (without https://)
- `DATABRICKS_CLIENT_ID`: OAuth client ID for authentication
- `DATABRICKS_CLIENT_SECRET`: OAuth client secret for authentication

### Setting up GitHub Secrets

1. Go to your repository on GitHub
2. Navigate to Settings > Secrets and variables > Actions
3. Click "New repository secret"
4. Add each of the required secrets listed above

### Local Testing

Before pushing, you can run the same tests locally:

```bash
# Install dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/ -v

# Run with coverage
pytest tests/ -v --cov=src/dabs --cov-report=term-missing
```

## Pre-Merge Checks Workflow

The `pre-merge-checks.yml` workflow runs comprehensive checks on every pull request and push to main branches to ensure code quality and project integrity.

### What It Does

#### 1. **Testing** (`test` job)
- Runs on multiple Python versions (3.9, 3.10, 3.11)
- Installs project dependencies
- Executes pytest with coverage reporting
- Uploads coverage reports to Codecov

#### 2. **Code Quality** (`lint` job)
- **Black**: Code formatting check
- **isort**: Import statement sorting
- **flake8**: Style and error checking
- **mypy**: Type checking

#### 3. **Build Validation** (`build` job)
- Builds the Python package
- Creates wheel and source distributions
- Uploads build artifacts

#### 4. **Databricks Validation** (`databricks-validation` job)
- Validates Databricks bundle configuration
- Performs dry-run bundle execution
- Only runs on pull requests

#### 5. **Security Scanning** (`security` job)
- **Bandit**: Security vulnerability scanning
- **Safety**: Dependency vulnerability checking
- Generates security reports

#### 6. **Dependency Review** (`dependency-review` job)
- Reviews dependency changes for security issues
- Fails on moderate or higher severity issues

### When It Runs

- **Pull Requests**: All jobs run to validate changes
- **Push to main branches**: All jobs run to ensure main branch integrity

### Requirements

The workflow requires the following secrets to be configured in your GitHub repository:

- **DATABRICKS_HOST**: Your Databricks workspace URL
- **DATABRICKS_TOKEN**: Personal access token for Databricks

### Local Development

To run the same checks locally before pushing:

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/ -v

# Run linting
black --check src/ tests/
isort --check-only src/ tests/
flake8 src/ tests/
mypy src/

# Run security checks
bandit -r src/
safety check

# Build package
python -m build
```

### Customization

#### Adding New Checks

To add new validation steps:

1. Add the tool to `requirements-dev.txt`
2. Add the step to the appropriate job in the workflow
3. Update the corresponding configuration files

#### Modifying Python Versions

Edit the matrix in the `test` job:

```yaml
strategy:
  matrix:
    python-version: ['3.9', '3.10', '3.11', '3.12']
```

#### Adjusting Linting Rules

- **Black**: Modify `pyproject.toml` or `.black.toml`
- **isort**: Update `pyproject.toml` or `.isort.cfg`
- **flake8**: Edit `.flake8`
- **mypy**: Modify `.mypy.ini` or `pyproject.toml`

### Troubleshooting

#### Common Issues

1. **Import errors in mypy**: Add missing packages to `.mypy.ini`
2. **Black formatting conflicts**: Run `black src/ tests/` locally
3. **isort conflicts**: Run `isort src/ tests/` locally
4. **Build failures**: Ensure `setup.py` is properly configured

#### Skipping Checks

To skip specific checks in a commit, use:

- `[skip ci]` in commit message to skip all checks
- `[skip lint]` to skip linting jobs
- `[skip test]` to skip testing jobs

### Performance Optimization

- **Dependency caching**: The workflow caches pip dependencies for faster builds
- **Parallel execution**: Jobs run in parallel where possible
- **Conditional execution**: Databricks validation only runs on PRs

### Integration with Other Tools

- **Codecov**: Coverage reporting integration
- **GitHub Security**: Dependency vulnerability alerts
- **Databricks CLI**: Bundle validation and testing
