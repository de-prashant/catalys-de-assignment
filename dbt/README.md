# dbt Setup for CATALYS Data Engineering Assignment

This folder contains the dbt-core setup for data transformation and modeling.

## Setup Instructions

1. **Activate Virtual Environment**:
   ```bash
   cd dbt
   venv\Scripts\activate  # On Windows
   # source venv/bin/activate  # On macOS/Linux
   ```

2. **Install Dependencies** (if not already installed):
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Profiles**:
   - Edit `~/.dbt/profiles.yml` with your Snowflake credentials.
   - Use environment variables for security.

4. **Test Connection**:
   ```bash
   dbt debug
   ```

5. **Run Models**:
   ```bash
   dbt run
   ```

## Project Structure
- `catalys_de_assignment/` — Main dbt project folder
- `venv/` — Virtual environment
- `requirements.txt` — Dependencies

## Notes
- The virtual environment is isolated to this folder for better dependency management.
- Ensure your `.env` file is in the root project directory for environment variables.