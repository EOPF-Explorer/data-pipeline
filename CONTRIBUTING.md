# Contributing

## Setup

```bash
git clone https://github.com/EOPF-Explorer/data-pipeline.git
cd data-pipeline
uv sync --all-extras
uv run pre-commit install
make test
```

**Requirements:** Python 3.11+, uv, kubectl (for integration tests)

## Testing

```bash
make test                              # All tests
pytest --cov=scripts --cov-report=html # With coverage
pytest tests/test_register_stac.py -v  # Specific file
```

**Coverage goal:** 80%+ on core scripts (current: 25%)

## Code Style

Automated via pre-commit: ruff (lint), ruff-format, mypy (types), yaml-check.

```bash
uv run pre-commit run --all-files     # All checks
uv run pre-commit run ruff --all-files # Specific check
```

**Type hints required:**
```python
def extract_item_id(stac_item: dict[str, Any]) -> str:  # ✅
    return stac_item["id"]
```

**Google-style docstrings:**
```python
def publish_message(config: Config, payload: dict[str, Any]) -> str:
    """Publish to RabbitMQ and trigger workflow.

    Args:
        config: RabbitMQ credentials
        payload: Workflow payload

    Returns:
        Item ID

    Raises:
        RuntimeError: If publish fails or connection times out
    """
```

## 📝 Commit Messages

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```bash
# Format: <type>(<scope>): <description>

# Types:
feat:     New feature
fix:      Bug fix
docs:     Documentation only
refactor: Code restructuring (no behavior change)
test:     Adding/updating tests
chore:    Maintenance (dependencies, configs)
perf:     Performance improvement
ci:       CI/CD changes

# Examples:
git commit -m "feat(stac): add TiTiler viewer links to STAC items"
git commit -m "fix(workflow): correct S3 credential mounting"
git commit -m "docs: update README with troubleshooting section"
git commit -m "test: add integration tests for AMQP publishing"
```

## 🔄 Pull Request Process

### Before Opening PR

- [ ] All tests pass: `make test`
- [ ] Pre-commit hooks pass: `uv run pre-commit run --all-files`
- [ ] Documentation updated (README, docstrings)
- [ ] CHANGELOG.md updated with changes
- [ ] Commit messages follow conventional format

### PR Checklist Template

When you open a PR, include:

```markdown
## Description
Brief description of what this PR does

## Type of Change
- [ ] Bug fix (non-breaking change fixing an issue)
- [ ] New feature (non-breaking change adding functionality)
- [ ] Breaking change (fix or feature causing existing functionality to change)
- [ ] Documentation update

## Testing
- [ ] Tests pass locally (`make test`)
- [ ] Pre-commit hooks pass
- [ ] Tested manually (describe steps)

## Screenshots (if applicable)
Add screenshots for UI/visual changes
```

### Review Process

1. Automated checks run (tests, linting)
2. At least one maintainer review required
3. Address feedback with new commits
4. Squash-merge after approval

## 🏗️ Project Structure

```
data-pipeline/
├── scripts/           # Core pipeline scripts
│   ├── publish_amqp.py
│   ├── register_stac.py
│   └── augment_stac_item.py
├── workflows/         # Argo Workflow templates
│   ├── geozarr-convert-template.yaml
│   └── payload.json
├── examples/          # Standalone examples and interactive tools
│   ├── simple_register.py
│   └── operator.ipynb
├── tests/            # Test suite
│   ├── test_register_stac.py
│   └── conftest.py
├── docker/           # Container definitions
└── pyproject.toml    # Dependencies and config
```

## 🐛 Reporting Bugs

### Before Reporting

1. Check existing issues
2. Verify it's reproducible
3. Test with latest code

### Bug Report Template

```markdown
**Describe the bug**
Clear description of what's wrong

**To Reproduce**
Steps to reproduce:
1. Run command '...'
2. See error

**Expected behavior**
What should happen

**Environment:**
- Python version: [e.g., 3.11.5]
- OS: [e.g., macOS 14.0]
- Kubernetes version: [e.g., 1.28]

**Logs**
```
Paste relevant logs here
```
```

## 💡 Feature Requests

We welcome feature ideas! Please:

1. Check if similar request exists
2. Describe use case clearly
3. Explain expected behavior
4. Consider implementation approach

## 📚 Documentation

### README Updates

When adding features, update:
- Quick Start section
- Usage examples
- Configuration options
- Troubleshooting

### Inline Documentation

- Add docstrings to all public functions
- Include type hints
- Explain non-obvious logic with comments
- Link to related documentation

## 🧑‍💻 Development Workflow

### Local Development Loop

```bash
# 1. Create feature branch
git checkout -b feat/my-feature

# 2. Make changes
# ... edit files ...

# 3. Run tests
make test

# 4. Format and lint
uv run pre-commit run --all-files

# 5. Commit
git add .
git commit -m "feat: add my feature"

# 6. Push and open PR
git push origin feat/my-feature
```

### Testing Changes

**For script changes:**
```bash
# Unit tests
pytest tests/test_my_script.py -v

# Integration test (requires cluster)
make test-e2e
```

**For workflow changes:**
```bash
# Deploy to test namespace
kubectl apply -f workflows/geozarr-convert-template.yaml -n test

# Trigger test run
kubectl create -f workflows/test-run.yaml -n test
```

**For notebook changes:**
```bash
# Launch notebook
make demo

# Test cells manually
# Verify outputs match expected results
```

## 🔐 Security

### Credentials

**Never commit:**
- API keys
- S3 credentials
- RabbitMQ passwords
- kubeconfig files

**Use instead:**
- Kubernetes secrets
- Environment variables
- `.env` files (in `.gitignore`)

### Reporting Vulnerabilities

Email security issues to: security@eopf-explorer.eu

## 📞 Getting Help

- **Questions**: Open a [GitHub Discussion](https://github.com/EOPF-Explorer/data-pipeline/discussions)
- **Bugs**: Open an [Issue](https://github.com/EOPF-Explorer/data-pipeline/issues)
- **Chat**: Join our Slack channel (request invite)

## 🎓 Learning Resources

### Recommended Reading

- [STAC Specification](https://stacspec.org/)
- [GeoZarr Spec](https://github.com/zarr-developers/geozarr-spec)
- [Argo Workflows Docs](https://argo-workflows.readthedocs.io/)
- [TiTiler Documentation](https://developmentseed.org/titiler/)

### Example Workflows

See `examples/operator.ipynb` for complete workflow example.

## 🙏 Thank You!

Your contributions make this project better for everyone. We appreciate your time and effort! 🚀
