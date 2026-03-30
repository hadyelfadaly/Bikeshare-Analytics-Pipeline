# 1. Start with the modern Airflow image
FROM apache/airflow:3.1.8

# 2. Switch to 'root' for system-level installations
USER root

# 3.copy uv from the official image to install it and then use it to install the requirements
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# 4. Install git (needed for dbt deps)
RUN apt-get update && apt-get install -y git && apt-get clean

# 5. Copy your requirements file
COPY requirements.txt .

# 6. Install all Python dependencies into system Python
RUN uv pip install --no-cache --system -r requirements.txt

# 7. Make dbt findable on PATH for the airflow user
ENV PATH="/usr/local/bin:/usr/python/bin:${PATH}"

# 8. Drop privileges for runtime
USER airflow