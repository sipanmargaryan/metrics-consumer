```sh
make help     # List available commands
make hooks    # Enable Git hooks from .githooks
make env      # Create .env from .env.example if missing
make sync     # Install locked local dependencies
make config   # Validate Docker Compose configuration
make migrate  # Apply database migrations locally (PostgreSQL must be running)
make up       # Build and start consumer + PostgreSQL
make down     # Stop services and preserve database data
make logs     # Follow consumer logs
make ps       # Show service status
make run      # Run locally (requires Kafka and migrated PostgreSQL)
make test     # Run tests; accepts ARGS="..."
make lint     # Check Ruff lint and formatting
make check    # Run lint checks and tests
```
