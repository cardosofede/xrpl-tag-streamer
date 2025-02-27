#!/bin/bash
# Setup script for XRPL Tag Streamer

# Create and activate a virtual environment
echo "Creating virtual environment..."
python -m venv .venv

# Detect shell and suggest activation command
SHELL_NAME=$(basename "$SHELL")
if [ "$SHELL_NAME" = "zsh" ] || [ "$SHELL_NAME" = "bash" ]; then
    echo "To activate the virtual environment, run:"
    echo "source .venv/bin/activate"
elif [ "$SHELL_NAME" = "fish" ]; then
    echo "To activate the virtual environment, run:"
    echo "source .venv/bin/activate.fish"
else
    echo "To activate the virtual environment, run the appropriate command for your shell"
fi

# Activate the virtual environment
source .venv/bin/activate

# Install dependencies directly
echo "Installing dependencies..."
pip install xrpl-py duckdb python-dotenv rich typer pydantic pandas

# Create data directory
mkdir -p data/logs

echo "Setup complete! You can now run the application."
echo "Remember to activate your virtual environment with: source .venv/bin/activate"
echo ""
echo "Example commands:"
echo "  python -m src.main stream"
echo "  python -m src.main history --days 1"
echo "  python -m src.main query"
echo "  python -m src.main stats" 