# 1. Start with the modern Airflow image
FROM apache/airflow:3.1.8

# 2. Switch to 'root' for system-level installations
USER root

# 3.copy uv from the official image to install it and then use it to install the requirements
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# 4. Switch back to 'airflow' user to install Python packages
USER airflow

# 5. Copy your requirements file
COPY requirements.txt .

# 6. Install uv and then install your requirements
RUN uv pip install --no-cache --system -r requirements.txt