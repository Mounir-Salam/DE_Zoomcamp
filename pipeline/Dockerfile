FROM python:3.13.11-slim

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
	# take /uk from the official image and store it in /bin/
	# Now uv becomes available when working inside the container

# Specify working directory
WORKDIR /code

# To avoid writing `ENTRYPOINT ["uv", "run", "python", "pipeline.py"]`
# Added virtual environment to PATH
ENV PATH="/code/.venv/bin:$PATH"

# Instead of `RUN pip install pandas pyarrow`
COPY pyproject.toml uv.lock ./
	# All these files should have been created by uv in /pipeline/ previously
	# will be copied to /code as it's the current working directory

# This will install dependencies from uv.lock
# This way we make sure dependencies we install are the same as the virtual env
RUN uv sync --locked

COPY pipeline.py ./

ENTRYPOINT ["python", "pipeline.py"]