#!/bin/bash
# FastTransfer MCP Server Installation Verification Script

echo "============================================"
echo "FastTransfer MCP Server - Installation Check"
echo "============================================"
echo ""

# Check Python version
echo "1. Checking Python version..."
python_version=$(python --version 2>&1)
echo "   $python_version"
if python -c "import sys; exit(0 if sys.version_info >= (3, 8) else 1)"; then
    echo "   ✅ Python version OK (3.8+)"
else
    echo "   ❌ Python 3.8+ required"
    exit 1
fi
echo ""

# Check if we're in the right directory
echo "2. Checking project structure..."
if [ -f "src/server.py" ] && [ -f "src/validators.py" ] && [ -f "src/fasttransfer.py" ]; then
    echo "   ✅ All source files present"
else
    echo "   ❌ Missing source files - are you in the project root?"
    exit 1
fi
echo ""

# Check dependencies
echo "3. Checking Python dependencies..."
missing_deps=()
for dep in mcp pydantic dotenv pytest; do
    if python -c "import $dep" 2>/dev/null; then
        echo "   ✅ $dep installed"
    else
        echo "   ❌ $dep missing"
        missing_deps+=("$dep")
    fi
done

if [ ${#missing_deps[@]} -gt 0 ]; then
    echo ""
    echo "   Install missing dependencies with:"
    echo "   pip install -r requirements.txt"
    exit 1
fi
echo ""

# Check FastTransfer binary
echo "4. Checking FastTransfer binary..."
if [ -f ".env" ]; then
    FASTTRANSFER_PATH=$(grep FASTTRANSFER_PATH .env | cut -d'=' -f2)
    if [ -z "$FASTTRANSFER_PATH" ]; then
        FASTTRANSFER_PATH="./fasttransfer/FastTransfer"
    fi
else
    FASTTRANSFER_PATH="./fasttransfer/FastTransfer"
fi

if [ -f "$FASTTRANSFER_PATH" ]; then
    if [ -x "$FASTTRANSFER_PATH" ]; then
        echo "   ✅ FastTransfer binary found and executable"
        echo "   Location: $FASTTRANSFER_PATH"
    else
        echo "   ⚠️  FastTransfer binary found but not executable"
        echo "   Run: chmod +x $FASTTRANSFER_PATH"
    fi
else
    echo "   ⚠️  FastTransfer binary not found at: $FASTTRANSFER_PATH"
    echo "   Update FASTTRANSFER_PATH in .env if binary is elsewhere"
fi
echo ""

# Run tests
echo "5. Running tests..."
if pytest tests/ -q 2>&1 | grep -q "passed"; then
    test_result=$(pytest tests/ -q 2>&1 | tail -1)
    echo "   ✅ $test_result"
else
    echo "   ❌ Tests failed - run 'pytest tests/ -v' for details"
    exit 1
fi
echo ""

# Test server import
echo "6. Testing server module..."
if python -c "from src import server" 2>/dev/null; then
    echo "   ✅ Server module imports successfully"
else
    echo "   ❌ Server module import failed"
    exit 1
fi
echo ""

# Check configuration
echo "7. Checking configuration..."
if [ -f ".env" ]; then
    echo "   ✅ .env file exists"
else
    echo "   ⚠️  .env file not found"
    echo "   Create from template: cp .env.example .env"
fi

if [ -f "example_config.json" ]; then
    echo "   ✅ example_config.json present"
else
    echo "   ⚠️  example_config.json missing"
fi
echo ""

# Final summary
echo "============================================"
echo "Installation Verification Complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "1. Ensure FastTransfer binary is at: $FASTTRANSFER_PATH"
echo "2. Copy .env.example to .env and configure paths"
echo "3. Add to Claude Code config (~/.claude.json):"
echo "   See example_config.json for template"
echo "4. Restart Claude Code"
echo "5. Test with: /mcp"
echo ""
echo "For help, see README.md"
