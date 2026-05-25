## Guidelines for Claude Code assistant


### Managing virtual environment and dependencies

**Production dependencies installation**
+ Always use `uv add <package_name>` to install packages.

**Running or installing development dependencies**

+ `uvx` (or `uv tool run`) - Run a command-line tool without installing it locally or globally
+ `uv run --with` - Inject a dependency temporarily just for one script execution.
+ `uv add --dev` - Permanently save a development tool (like a linter or test framework) to your project's pyproject.toml.

**Running scripts**

+ Run scripts or cli commands from within a virtual environment like so: `uv run python script.py` from the root working directory. Always ensure it's activated by running `source .venv/bin/activate`. That way, any installed dependencies and cli tools will work as expected. 


### Using Dagster project

Dagster has its own virtual environment under `dagster-orch/` directory. It can be activated with `source .venv/bin/activate` inside that Dagster directory. Don't mix up virtual environments in order to avoid installing wrong dependencies in incorrect virtual environments.

Virtual environment rules:
- Dagster venv: `cd dagster-orch && source .venv/bin/activate` — for dg commands, dagster components, and other built-in functionality


**Troubleshooting Dagster:**
+ If any issues are encountered while creating assets in Dagster, ask `/dagster-expert` plugin for help.

---

### Handling secret credentials

API credentials are stored in `dagster-orch/.env` which is gitignored.

CRITICAL: never ask for credentials in chat. Always let the user edit secrets directly and do not attempt to read them.

---

