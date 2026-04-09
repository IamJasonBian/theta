FROM python:3.11-slim

WORKDIR /app

# Copy only what the API needs — no requirements file because the app is
# stdlib-only (Airflow is stubbed at runtime when not present).
COPY . /app/operators/

ENV PYTHONUNBUFFERED=1 \
    HOST=0.0.0.0 \
    PORT=8765
EXPOSE 8765

# Healthcheck hits the same $PORT the app binds to so it works on Render
# (which injects its own $PORT) and locally (falls back to 8765).
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import os,urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen(f\"http://127.0.0.1:{os.environ.get('PORT','8765')}/healthz\", timeout=2).status==200 else 1)"

# No positional args → api.py reads $HOST / $PORT from the environment.
CMD ["python", "operators/api.py"]
